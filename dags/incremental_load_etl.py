from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import os
import pandas as pd
import logging

# Inisialisasi logger untuk Airflow task
logger = logging.getLogger("airflow.task")

"""
DAG ini digunakan untuk melakukan incremental load data dari database source PostgreSQL ke Snowflake. 
Hanya data baru atau yang mengalami perubahan (updated) sejak eksekusi terakhir yang akan ditarik.
Proses merge/upsert dilakukan pada DWH Snowflake menggunakan perintah MERGE.
"""

default_args = {
    "owner": "DEAssessment",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "retries": 0,
}

## 1. Ekstraksi Data Incremental dari PostgreSQL
def extract_incremental_data(**context):
    logger.info("Memulai proses ekstraksi data secara incremental dari PostgreSQL...")
    
    # Mendapatkan execution/logical date untuk interval saat ini dari Airflow context
    # Jika dijalankan manual pertama kali, default ke 1 hari yang lalu
    data_interval_start = context.get('data_interval_start')
    if data_interval_start:
        last_load_time = data_interval_start.to_datetime_string()
    else:
        # Fallback jika tidak ada interval start (misal ad-hoc manual run tanpa parameter)
        last_load_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        
    logger.info(f"Menggunakan threshold updated_at >= '{last_load_time}' untuk ekstraksi incremental.")

    pg_hook = PostgresHook(postgres_conn_id="postgres_source")

    ### Ekstraksi authors
    author_query = "SELECT * FROM authors WHERE updated_at >= %s"
    df_authors = pg_hook.get_pandas_df(author_query, parameters=(last_load_time,))
    author_path_file = "/tmp/incremental_authors.csv"
    df_authors.to_csv(author_path_file, index=False, sep='|')
    logger.info(f"Berhasil mengekstrak {len(df_authors)} data baru/diperbarui dari tabel authors.")

    ### Ekstraksi articles
    articles_query = "SELECT * FROM articles WHERE updated_at >= %s"
    df_articles = pg_hook.get_pandas_df(articles_query, parameters=(last_load_time,))
    articles_path_file = "/tmp/incremental_articles.csv"
    df_articles.to_csv(articles_path_file, index=False, sep='|')
    logger.info(f"Berhasil mengekstrak {len(df_articles)} data baru/diperbarui dari tabel articles.")

## 2. Load data incremental ke Snowflake staging tables
def load_incremental_to_staging():
    logger.info("Memulai proses memuat data incremental ke Snowflake staging tables...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    # Truncate staging table karena hanya menampung delta/batch incremental saat ini
    sf_hook.run("TRUNCATE TABLE staging.authors")
    sf_hook.run("TRUNCATE TABLE staging.articles")
    logger.info("Staging tables (authors dan articles) berhasil di-truncate.")

    ### Load authors
    author_path_file = "/tmp/incremental_authors.csv"
    if os.path.exists(author_path_file):
        if os.path.getsize(author_path_file) > 40: # Cek jika ada datanya (bukan cuma header)
            logger.info("Mengunggah data incremental authors ke Snowflake User Stage...")
            sf_hook.run(f'PUT file://{author_path_file} @~ OVERWRITE = TRUE;')
            sf_hook.run("""
                COPY INTO staging.authors
                FROM @~/incremental_authors.csv
                FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
                PURGE = TRUE;
            """)
            count_authors = sf_hook.get_first("select count(*) from staging.authors")[0]
            logger.info(f"Staging authors berhasil dimuat dengan {count_authors} data delta.")
        else:
            logger.info("Tidak ada data baru/diperbarui untuk authors pada batch ini.")
    else:
        raise FileNotFoundError("File CSV incremental authors tidak ditemukan.")

    ### Load articles
    articles_path_file = "/tmp/incremental_articles.csv"
    if os.path.exists(articles_path_file):
        if os.path.getsize(articles_path_file) > 65: # Cek jika ada datanya
            logger.info("Mengunggah data incremental articles ke Snowflake User Stage...")
            sf_hook.run(f'PUT file://{articles_path_file} @~ OVERWRITE = TRUE;')
            sf_hook.run("""
                COPY INTO staging.articles
                FROM @~/incremental_articles.csv
                FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
                PURGE = TRUE;
            """)
            count_articles = sf_hook.get_first("select count(*) from staging.articles")[0]
            logger.info(f"Staging articles berhasil dimuat dengan {count_articles} data delta.")
        else:
            logger.info("Tidak ada data baru/diperbarui untuk articles pada batch ini.")
    else:
        raise FileNotFoundError("File CSV incremental articles tidak ditemukan.")

## 3. Merge dim_authors (Upsert)
def merge_dim_authors():
    logger.info("Memulai proses MERGE (Upsert) ke dwh.dim_authors...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    # Cek apakah ada data di staging sebelum melakukan MERGE
    count_staging = sf_hook.get_first("select count(*) from staging.authors")[0]
    if count_staging == 0:
        logger.info("Staging authors kosong. Melewati proses MERGE untuk dwh.dim_authors.")
        return

    sf_hook.run("""
        MERGE INTO dwh.dim_authors target
        USING staging.authors source
        ON target.author_id = source.id
        WHEN MATCHED THEN
            UPDATE SET target.name = source.name,
                       target.email = source.email,
                       target.created_at = source.created_at,
                       target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (author_id, name, email, created_at, updated_at)
            VALUES (source.id, source.name, source.email, source.created_at, source.updated_at);
    """)
    logger.info("Proses MERGE ke dwh.dim_authors selesai.")

## 4. Merge dim_articles (Upsert)
def merge_dim_articles():
    logger.info("Memulai proses MERGE (Upsert) ke dwh.dim_articles...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    # Cek apakah ada data di staging
    count_staging = sf_hook.get_first("select count(*) from staging.articles")[0]
    if count_staging == 0:
        logger.info("Staging articles kosong. Melewati proses MERGE untuk dwh.dim_articles.")
        return

    sf_hook.run("""
        MERGE INTO dwh.dim_articles target
        USING staging.articles source
        ON target.article_id = source.id
        WHEN MATCHED THEN
            UPDATE SET target.title = source.title,
                       target.content = source.content,
                       target.published_at = source.published_at,
                       target.created_at = source.created_at,
                       target.updated_at = source.updated_at,
                       target.deleted_at = source.deleted_at
        WHEN NOT MATCHED THEN
            INSERT (article_id, title, content, published_at, created_at, updated_at, deleted_at)
            VALUES (source.id, source.title, source.content, source.published_at, source.created_at, source.updated_at, source.deleted_at);
    """)
    logger.info("Proses MERGE ke dwh.dim_articles selesai.")

## 5. Merge fact_reports_articles (Upsert/Delete)
def merge_fact_report_articles():
    logger.info("Memulai proses MERGE ke dwh.fact_reports_articles...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    # Cek staging articles
    count_staging = sf_hook.get_first("select count(*) from staging.articles")[0]
    if count_staging == 0:
        logger.info("Staging articles kosong. Melewati proses MERGE untuk dwh.fact_reports_articles.")
        return

    # Gunakan query MERGE yang dapat menangani insert baru, update data aktif,
    # serta menghapus (DELETE) artikel yang menjadi draft (published_at NULL) atau di-soft delete (deleted_at NOT NULL)
    sf_hook.run("""
        MERGE INTO dwh.fact_reports_articles target
        USING (
            SELECT a.id as article_id
                 , au.author_id as author_id
                 , a.published_at
                 , COUNT(*) as article_count
                 , MAX(a.deleted_at) as deleted_at
                 , MAX(a.created_at) as created_at
                 , MAX(a.updated_at) as updated_at
            FROM staging.articles a
            LEFT JOIN dwh.dim_authors au ON a.author_id = au.author_id
            GROUP BY a.id, au.author_id, a.published_at
        ) source
        ON target.article_id = source.article_id
        WHEN MATCHED AND (source.published_at IS NULL OR source.deleted_at IS NOT NULL) THEN
            DELETE
        WHEN MATCHED AND source.published_at IS NOT NULL AND source.deleted_at IS NULL THEN
            UPDATE SET target.author_id = source.author_id,
                       target.published_date_at = TO_NUMBER(TO_CHAR(source.published_at, 'YYYYMMDD')),
                       target.article_count = source.article_count
                       target.created_at = source.created_at,
                       target.updated_at = source.updated_at
        WHEN NOT MATCHED AND source.published_at IS NOT NULL AND source.deleted_at IS NULL THEN
            INSERT (article_id, author_id, published_date_at, article_count, created_at, updated_at)
            VALUES (source.article_id, source.author_id, TO_NUMBER(TO_CHAR(source.published_at, 'YYYYMMDD')), source.article_count, source.created_at, source.updated_at);
    """)
    logger.info("Proses MERGE ke dwh.fact_reports_articles selesai.")

# Membuat DAG Incremental
with DAG(
    "incremental_load_etl",
    default_args=default_args,
    description="DAG untuk melakukan incremental load data dari PostgreSQL ke Snowflake",
    schedule="@hourly", # Dijalankan manual atau terjadwal (misal: daily)
    catchup=False,
    tags=["incremental_load", "postgresql", "snowflake"]
) as dag:

    extract_task = PythonOperator(
        task_id='extract_incremental_data',
        python_callable=extract_incremental_data,
        execution_timeout=timedelta(minutes=3),
    )

    load_staging_task = PythonOperator(
        task_id='load_incremental_to_staging',
        python_callable=load_incremental_to_staging,
        execution_timeout=timedelta(minutes=3),
    )

    merge_dim_authors_task = PythonOperator(
        task_id='merge_dim_authors',
        python_callable=merge_dim_authors,
        execution_timeout=timedelta(minutes=3),
    )

    merge_dim_articles_task = PythonOperator(
        task_id='merge_dim_articles',
        python_callable=merge_dim_articles,
        execution_timeout=timedelta(minutes=3),
    )

    merge_fact_report_articles_task = PythonOperator(
        task_id='merge_fact_report_articles',
        python_callable=merge_fact_report_articles,
        execution_timeout=timedelta(minutes=3),
    )

    # Urutan eksekusi task:
    # Ekstraksi -> Staging -> Dimensi (Paralel) -> Fakta
    extract_task >> load_staging_task >> [merge_dim_authors_task, merge_dim_articles_task] >> merge_fact_report_articles_task

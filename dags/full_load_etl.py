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
DAG ini digunakan untuk melakukan full load data dari database source PostgreSQL ke Snowflake. 
Untuk kebutuhan backfill, maupun kebutuhan lainnya, DAG ini akan dijalankan secara manual. 
Alur ETL/ELT yang dilakukan adalah sebagai berikut:
"""

# Membuat default arguments untuk DAG
default_args = {
    "owner": "DEAssessment",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "retries": 0,
}

# Membuat Alur ETL/ELT

## 1. Ekstraksi Data dari database source PostgreSQL
def extract_data():
    logger.info("Memulai proses ekstraksi data dari PostgreSQL...")
    ### membuat koneksi hook ke database PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_source")

    ### query untuk mengambil data dari tabel authors dan menyimpannya ke dalam file CSV
    author_data = "Select * from authors"
    df_authors = pg_hook.get_pandas_df(author_data)
    author_path_file = "/tmp/authors.csv"
    df_authors.to_csv(author_path_file, index=False, sep='|')
    logger.info(f"Berhasil mengekstrak data dari tabel authors. Total baris: {len(df_authors)}")

    ### query untuk mengambil data dari tabel articles dan menyimpannya ke dalam file CSV
    articles_data = "Select * from articles"
    df_articles = pg_hook.get_pandas_df(articles_data)
    articles_path_file = "/tmp/articles.csv"
    df_articles.to_csv(articles_path_file, index=False, sep='|')
    logger.info(f"Berhasil mengekstrak data dari tabel articles. Total baris: {len(df_articles)}")

## 2. Load data ke Snowflake staging tables
def load_data_to_staging():
    logger.info("Memulai proses memuat data ke Snowflake staging tables...")
    ### membuat koneksi hook ke Snowflake
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    ### truncate staging tables sebelum melakukan load data
    sf_hook.run("TRUNCATE TABLE staging.authors")
    sf_hook.run("TRUNCATE TABLE staging.articles")
    logger.info("Staging tables (authors dan articles) berhasil di-truncate.")

    ### Load data dari file csv ke staging tables snowflake
    #### melakukan load data authors ke staging_authors
    author_path_file = "/tmp/authors.csv"
    if os.path.exists(author_path_file):
        logger.info("Mengunggah dan menyalin data authors ke staging...")
        sf_hook.run(f'PUT file://{author_path_file} @~ OVERWRITE = TRUE;')
        sf_hook.run("""
            COPY INTO staging.authors
            FROM @~/authors.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
            PURGE = TRUE;
        """)

        count_authors = sf_hook.get_first("select count(*) from staging.authors")[0]
        logger.info(f"Jumlah data yang berhasil di-load ke staging_authors: {count_authors} rows")

    else:
        raise FileNotFoundError("File CSV tidak ditemukan di path yang ditentukan.")
    
    #### melakukan load data articles ke staging_articles
    articles_path_file = "/tmp/articles.csv"
    if os.path.exists(articles_path_file):
        logger.info("Mengunggah dan menyalin data articles ke staging...")
        sf_hook.run(f'PUT file://{articles_path_file} @~ OVERWRITE = TRUE;')
        sf_hook.run("""
            COPY INTO staging.articles
            FROM @~/articles.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
            PURGE = TRUE;
        """)
        count_articles = sf_hook.get_first("select count(*) from staging.articles")[0]
        logger.info(f"Jumlah data yang berhasil di-load ke staging_articles: {count_articles} rows")
    else:
        raise FileNotFoundError("File CSV tidak ditemukan di path yang ditentukan.")

## 3. Load dim_authors ke Snowflake target tables
def load_dim_authors():
    logger.info("Memulai proses load data ke Snowflake dwh.dim_authors...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")
    sf_hook.run("TRUNCATE TABLE dwh.dim_authors")
    logger.info("Tabel dwh.dim_authors berhasil di-truncate.")

    sf_hook.run("""
        INSERT INTO dwh.dim_authors (author_id, name, email, created_at, updated_at)
        SELECT id as author_id
                , name
                , email
                , created_at
                , updated_at
        FROM staging.authors
    """)
    count_dim_authors = sf_hook.get_first("select count(*) from dwh.dim_authors")[0]
    logger.info(f"Jumlah data yang berhasil disimpan di dwh.dim_authors: {count_dim_authors} rows")

## 4. Load dim_articles ke Snowflake target tables
def load_dim_articles():
    logger.info("Memulai proses load data ke Snowflake dwh.dim_articles...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")
    sf_hook.run("TRUNCATE TABLE dwh.dim_articles")
    logger.info("Tabel dwh.dim_articles berhasil di-truncate.")

    sf_hook.run("""
        INSERT INTO dwh.dim_articles (article_id, title, content, published_at, created_at, updated_at, deleted_at)
        SELECT id as article_id
                , title
                , content
                , published_at
                , created_at
                , updated_at
                , deleted_at
        FROM staging.articles;
    """)
    count_dim_articles = sf_hook.get_first("select count(*) from dwh.dim_articles")[0]
    logger.info(f"Jumlah data yang berhasil disimpan di dwh.dim_articles: {count_dim_articles} rows")

## 5. Load fact_reports_articles ke Snowflake target tables
def load_fact_report_articles():
    logger.info("Memulai proses load data ke Snowflake dwh.fact_reports_articles...")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")
    sf_hook.run("TRUNCATE TABLE dwh.fact_reports_articles")
    logger.info("Tabel dwh.fact_reports_articles berhasil di-truncate.")

    sf_hook.run("""
        INSERT INTO dwh.fact_reports_articles (article_id, author_id, published_date_at, article_count, created_at, updated_at)
        SELECT 
                a.id as article_id
                , au.author_id
                , TO_NUMBER(TO_CHAR(a.published_at, 'YYYYMMDD')) as published_date_at
                , COUNT(a.id) as article_count
                , MAX(a.created_at) as created_at
                , MAX(a.updated_at) as updated_at
        from staging.articles a
        JOIN dwh.dim_authors au
        ON a.author_id = au.author_id
        WHERE a.published_at IS NOT NULL and a.deleted_at IS NULL
        GROUP BY a.id, au.author_id, a.published_at;
    """)
    count_fact_articles = sf_hook.get_first("select count(*) from dwh.fact_reports_articles")[0]
    logger.info(f"Jumlah data yang berhasil disimpan di dwh.fact_reports_articles: {count_fact_articles} rows")

# Membuat DAG untuk full load data dari database source PostgreSQL ke Snowflake
with DAG(
    "full_load_etl",
    default_args=default_args,
    description="DAG untuk melakukan full load data dari database source PostgreSQL ke Snowflake",
    schedule=None,
    catchup=False,
    tags = ["full_load", "postgresql", "snowflake"]
) as dag:

    extract_task = PythonOperator(
        task_id = 'extract_data_from_postgres',
        python_callable = extract_data,
        execution_timeout = timedelta(minutes=3),
        dag=dag
    )

    load_staging_task = PythonOperator(
        task_id = 'load_data_to_staging',
        python_callable = load_data_to_staging,
        execution_timeout = timedelta(minutes=3),
        dag=dag
    )

    load_dim_authors_task = PythonOperator(
        task_id = 'load_dim_authors',
        python_callable = load_dim_authors,
        execution_timeout = timedelta(minutes=3),
        dag=dag
    )

    load_dim_articles_task = PythonOperator(
        task_id = 'load_dim_articles',
        python_callable = load_dim_articles,
        execution_timeout = timedelta(minutes=3),
        dag=dag
    )

    load_fact_report_articles_task = PythonOperator(
        task_id = 'load_fact_report_articles',
        python_callable = load_fact_report_articles,
        execution_timeout = timedelta(minutes=3),
        dag=dag
    )

    ### Menentukan urutan eksekusi task
    extract_task >> load_staging_task >> [load_dim_authors_task, load_dim_articles_task] >> load_fact_report_articles_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import os
import pandas as pd

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

## Ekstraksi Data dari database source PostgreSQL
def extract_data():
    ### membuat koneksi hook ke database PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_source")

    ### query untuk mengambil data dari tabel authors dan menyimpannya ke dalam file CSV
    author_data = "Select * from authors"
    df_authors = pg_hook.get_pandas_df(author_data)
    author_path_file = "/tmp/authors.csv"
    df_authors.to_csv(author_path_file, index=False)

    ### query untuk mengambil data dari tabel articles dan menyimpannya ke dalam file CSV
    articles_data = "Select * from articles"
    df_articles = pg_hook.get_pandas_df(articles_data)
    articles_path_file = "/tmp/articles.csv"
    df_articles.to_csv(articles_path_file, index=False)

## Load data ke Snowflake staging tables
def load_data_to_staging():
    ### membuat koneksi hook ke Snowflake
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    ### truncate staging tables sebelum melakukan load data
    sf_hook.run("TRUNCATE TABLE staging.authors")
    sf_hook.run("TRUNCATE TABLE staging.articles")

    ### Load data dari file csv ke staging tables snowflake
    #### melakukan load data authors ke staging_authors
    author_path_file = "/tmp/authors.csv"
    if os.path.exists(author_path_file):
        sf_hook.run(f'PUT file://{author_path_file} @~ OVERWRITE = TRUE;')
        sf_hook.run("""
            COPY INTO staging.authors
            FROM @~/authors.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            PURGE = TRUE;
        """)

        count_authors = sf_hook.get_first("select count(*) from staging.authors")[0]
        print(f"Jumlah data yang berhasil di load ke staging_authors : {count_authors} rows")

    else:
        raise FileNotFoundError("File CSV tidak ditemukan di path yang ditentukan.")
    
    #### melakukan load data articles ke staging_articles
    articles_path_file = "/tmp/articles.csv"
    if os.path.exists(articles_path_file):
        sf_hook.run(f'PUT file://{articles_path_file} @~ OVERWRITE = TRUE;')
        sf_hook.run("""
            COPY INTO staging.articles
            FROM @~/articles.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
            PURGE = TRUE;
        """)
        count_articles = sf_hook.get_first("select count(*) from staging.articles")[0]
        print(f"Jumlah data yang berhasil di load ke staging_articles : {count_articles} rows")
    else:
        raise FileNotFoundError("File CSV tidak ditemukan di path yang ditentukan.")

## Transformasi data dan load ke Snowflake target tables
def merge_data_to_target():
    ### membuat koneksi hook ke Snowflake
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_target")

    ### Melakukan truncate pada target tables sebelum melakukan merge data
    sf_hook.run("TRUNCATE TABLE dwh.dim_authors")
    sf_hook.run("TRUNCATE TABLE dwh.dim_articles")
    sf_hook.run("TRUNCATE TABLE dwh.fact_reports_articles")

    ### melakukan merge data dari staging tables ke target tables
    #### melakukan merge data dari staging_authors ke dim_authors
    sf_hook.run("""
        INSERT INTO dwh.dim_authors (authors_id, name, email, created_at, updated_at) as 
        SELECT id as authors_id
                , name
                , email
                , created_at
                , updated_at
        FROM staging.authors
    """)

    #### melakukan merge data dari staging_articles ke dim_articles
    sf_hook.run("""
        INSERT INTO dwh.dim_articles (articles_id, title, content, published_at, created_at, updated_at) as 
        SELECT id as articles_id
                , title
                , content
                , published_at
                , created_at
                , updated_at
        FROM staging.articles;
    """)

    #### melakukan merge data ke fact_reports_articles
    sf_hook.run("""
        INSERT INTO dwh.fact_reports_articles (article_id, author_id, published_date_at, article_count) as
        SELECT a.articles_id
                , au.authors_id
                , a.published_at as published_date_at
                , count(a.id) as article_count
        from dwh.dim_articles a
        JOIN dwh.dim_authors au
        ON a.articles_id = au.authors_id
        GROUP BY a.articles_id, au.authors_id, a.published_at;
    """)

## Membuat DAG untuk full load data dari database source PostgreSQL ke Snowflake
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
    merge_target_task = PythonOperator(
        task_id = 'merge_data_to_target',
        python_callable = merge_data_to_target,
        execution_timeout = timedelta(minutes=3),
        dag=dag
    )

    ### Menentukan urutan eksekusi task
    extract_task >> load_staging_task >> merge_target_task


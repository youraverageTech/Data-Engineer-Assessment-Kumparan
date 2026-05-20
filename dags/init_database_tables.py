from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import os

# Definisikan path ke folder include untuk pencarian file SQL
# Secara default di Astronomer/Airflow, file dikonfigurasi melalui template_searchpath
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
INCLUDE_DIR = os.path.join(AIRFLOW_HOME, 'include')

default_args = {
    'owner': 'DEAssessment',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'init_database_tables',
    default_args=default_args,
    description='DAG untuk menjalankan file SQL di folder include untuk inisialisasi database',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=[INCLUDE_DIR],
    tags=['setup', 'postgresql', 'snowflake'],
) as dag:

    # Task untuk menjalankan source_table_creation.sql pada PostgreSQL
    setup_source = SQLExecuteQueryOperator(
        task_id='setup_source_postgres',
        conn_id='postgres_source',
        sql='source_table_creation.sql',
    )

    # Task untuk menjalankan seed_data_source.sql pada PostgreSQL
    seed_source = SQLExecuteQueryOperator(
        task_id='seed_source_postgres',
        conn_id='postgres_source',
        sql='seed_data_source.sql',
    )

    # Task untuk menjalankan target_table_creation.sql pada Snowflake
    setup_target = SQLExecuteQueryOperator(
        task_id='setup_target_snowflake',
        conn_id='snowflake_target',
        sql='target_table_creation.sql',
    )

    # Menentukan urutan eksekusi: Source dulu, kemudian Seed Source, baru Target
    setup_source >> seed_source >> setup_target

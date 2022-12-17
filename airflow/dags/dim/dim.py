from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args= {
    'owner' : 'faris'
}

with DAG(
    "dim_table",
    start_date = datetime(22,11,11),
    schedule_interval = None,
    default_args= default_args
) as dag:

    start_job= DummyOperator(
        task_id= 'start_job'
    )

    dim_currency= PostgresOperator(
        task_id='dim_currency',
        postgres_conn_id="postgres_conn",
        sql='/opt/airflow/scripts/sql/dim_currency.sql',
    )

    dim_country= PostgresOperator(
        task_id='dim_country',
        postgres_conn_id="postgres_conn",
        sql='/opt/airflow/scripts/sql/dim_country.sql',
    )

    dim_state= PostgresOperator(
        task_id='dim_state',
        postgres_conn_id="postgres_conn",
        sql='/opt/airflow/scripts/sql/dim_state.sql',
    )

    dim_city= PostgresOperator(
        task_id='dim_city',
        postgres_conn_id="postgres_conn",
        sql='/opt/airflow/scripts/sql/dim_city.sql',
    )

    finish_job= DummyOperator(
        task_id= 'finish_job'
    )

    dim_currency 
    start_job >> dim_country >> dim_state >> dim_city >> finish_job
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args= {
    'owner' : 'faris'
}

with DAG(
    "fact_daily",
    start_date = datetime(22,11,11),
    schedule_interval = None,
    default_args= default_args
) as dag:

    start_job= DummyOperator(
        task_id= 'start_job'
    )

    fact_total_state= PostgresOperator(
        task_id='fact_total_state',
        postgres_conn_id="postgres_default",
        sql='/opt/airflow/scripts/sql/fact_total_state.sql',
    )

    fact_currency_daily= PostgresOperator(
        task_id='fact_currency_daily',
        postgres_conn_id="postgres_default",
        sql='/opt/airflow/scripts/sql/fact_currency_daily_avg.sql',
    )

    finish_job= DummyOperator(
        task_id= 'finish_job'
    )

     
    start_job >> fact_total_state >> fact_currency_daily >> finish_job
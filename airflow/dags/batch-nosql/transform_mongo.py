from datetime import datetime

# from airflow.operators.bash_operator import Bashoperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow import DAG

default_args= {
    'owner' : 'faris'
}

with DAG(
    "transform_mongo",
    start_date = datetime(22,11,11),
    schedule_interval = None,
    default_args= default_args
) as dag:

    # start job
    job_start = DummyOperator(
        task_id="job_start"
    )

    # extract data
    transform_mongo= BashOperator(
    task_id= 'transform_mongo',
    bash_command= 'python3 /opt/airflow/scripts/transform_mongo.py'
    )

    # finish job
    job_finish = DummyOperator(
        task_id="finish"
    )

    # Orchestration
    (
        job_start
        >> transform_mongo
        >> job_finish
    )
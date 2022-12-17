from datetime import datetime

# from airflow.operators.bash_operator import Bashoperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

from airflow import DAG

default_args= {
    'owner' : 'faris'
}

with DAG(
    "csv_to_mysql",
    start_date = datetime(22,11,11),
    schedule_interval = None,
    default_args= default_args
) as dag:

    # start job
    job_start = DummyOperator(
        task_id="job_start"
    )

    # extract data
    csv_to_mysql = SparkSubmitOperator(
        task_id='csv_to_mysql',
        name='get csv',
        verbose=1,
        conf={'spark.master':"spark://spark:7077"},
        conn_id='spark_connection',
        application='/usr/local/spark/app/csv_to_mysql.py',
        jars='/usr/local/spark/resources/mysql-connector-j-8.0.31.jar'
    )

    # load data
    mysql_to_DWH = SparkSubmitOperator(
        task_id='mysql_to_DWH',
        name='Load data',
        verbose=1,
        conf={'spark.master':"spark://spark:7077"},
        conn_id='spark_connection',
        application='/usr/local/spark/app/mysql_to_postgres.py',
        jars=['/usr/local/spark/resources/mysql-connector-j-8.0.31.jar','/usr/local/spark/resources/postgresql-42.5.1.jar']
    )

    # finish job
    job_finish = DummyOperator(
        task_id="finish"
    )

    # Orchestration
    (
        job_start
        >> csv_to_mysql
        >> mysql_to_DWH
        >> job_finish
    )
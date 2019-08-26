
import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from vn_stock.tasks import etl_vndirect


def hello_world():
    logging.info("Hello World")


#
# TODO: Add a daily `schedule_interval` argument to the following DAG
#

dag = DAG(
    "vn_stock.etl",
    start_date=datetime.datetime.now() - datetime.timedelta(days=60),
    schedule_interval='@once')

task = PythonOperator(
    task_id="stock_ingestion",
    python_callable=etl_vndirect.main,
    dag=dag)

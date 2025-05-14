# dags/test_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='example_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=["example"],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end
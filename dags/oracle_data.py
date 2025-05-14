from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from utils.data_processor import DataProcessor


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='oracle_data_processing_dag_cfg',
    default_args=default_args,
    schedule_interval='1 * * * *',
    start_date=days_ago(1),
    catchup=False,
    # tags=['oracle', 'config', 'taskflow'],
)
def dag_main():
    processor = DataProcessor()

    @task()
    def query_data():
        return processor.query_data()

    @task()
    def filter_today(df: pd.DataFrame):
        return processor.filter_today_block(df)

    @task()
    def update(df: pd.DataFrame):
        return processor.update_status(df)

    df1 = query_data()
    df2 = filter_today(df1)
    update(df2)

dag_instance = dag_main()
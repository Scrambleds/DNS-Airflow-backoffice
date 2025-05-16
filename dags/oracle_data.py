from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from utils.data_processor import DataProcessor

default_args = {
    'owner': 'airflow',
    'retries': 20,
    'retry_delay': timedelta(minutes=5),
    "depends_on_past": False
}

@dag(
    dag_id='oracle_data_processing_dag_cfg',
    default_args=default_args,
    schedule_interval='* 20 * * *',
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
    def update(df: pd.DataFrame):
        return processor.update_status(df)

    df1 = query_data()
    update(df1)

dag_instance = dag_main()
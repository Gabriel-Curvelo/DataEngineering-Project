from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Path to Airflow import scripts
sys.path.append("/usr/local/airflow/include/scripts") 

from brewery_api_ingestion import brewery_api
from silverlayer_output import main as silver_main
from goldlayer_output import main as gold_main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "brewery",
    default_args=default_args,
    description="Coleta dados da API OpenBreweryDB e salva no MinIO",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # fetch_task = PythonOperator(
    #     task_id="brewery_api",
    #     python_callable=brewery_api,
    # )

    transform_silver_task = PythonOperator(
        task_id="silver_layer",
        python_callable=silver_main,
    )

    transform_gold_task = PythonOperator(
        task_id="gold_layer",
        python_callable=gold_main,
    )

    # Define execution order: bronze → silver → gold
    transform_silver_task >> transform_gold_task

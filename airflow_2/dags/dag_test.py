from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add src folder to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from extract.fetch_api_data import fetch_data_from_api
from load.gcs_uploader import upload_to_gcs
from load.bq_loader import load_to_bigquery
from utils.config import API_URL, BUCKET_NAME, BQ_DATASET, BQ_TABLE
from utils.helpers import generate_filename

def pipeline():
    data = fetch_data_from_api(API_URL)
    filename = generate_filename("api_data")
    upload_to_gcs(data, filename, BUCKET_NAME)
    load_to_bigquery(filename, BUCKET_NAME, BQ_DATASET, BQ_TABLE)

with DAG(
    dag_id="api_to_bq_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["api", "gcs", "bigquery"]
) as dag:

    run_pipeline = PythonOperator(
        task_id="run_api_to_bq_pipeline",
        python_callable=pipeline
    )

    run_pipeline
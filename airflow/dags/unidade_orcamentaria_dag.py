import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from tasks.download_ano import download_csv_from_url
from tasks.upload_ano import upload_csv_to_gcs

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

load_dotenv()

GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET_PROD")
BQ_TABLE = os.getenv("BQ_TABLE_UN_ORCAMENTARIA")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="unidade_orcamentaria",
    default_args=default_args,
    schedule_interval="@yearly",
    catchup=True,
)
def unidade_orcamentaria_dag():
    @task()
    def download_task(**kwargs):
        logical_date = kwargs["logical_date"]
        download_csv_from_url(
            logical_date,
            conjunto="unidade_orcamentaria",
            local_csv_path="/tmp/unidade_orcamentaria_dadospb.csv",
        )
        return "/tmp/unidade_orcamentaria_dadospb.csv"

    @task()
    def upload_task(local_csv_path: str, **kwargs):
        logical_date = kwargs["logical_date"]
        upload_csv_to_gcs(
            logical_date,
            conjunto="unidade_orcamentaria",
            local_csv_path=local_csv_path,
            gcp_bucket_name=GCP_BUCKET_NAME,
            gcp_conn_id="google_cloud_default",
        )

    @task()
    def carregar_para_bq_task(**kwargs):
        logical_date = kwargs["logical_date"]
        time.sleep(10)
        carregar_para_bq = GCSToBigQueryOperator(
            task_id="carregar_csv_para_bq",
            bucket=GCP_BUCKET_NAME,
            source_objects=[f"unidade_orcamentaria/dadospb_{logical_date.year}.csv"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
            field_delimiter=";",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            gcp_conn_id="google_cloud_default",
        )

        carregar_para_bq.execute(kwargs)

    local_file_path = download_task()
    gcs_file_path = upload_task(local_file_path)
    carregar_para_bq_task().set_upstream(gcs_file_path)


unidade_orcamentaria_dag()

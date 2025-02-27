import os
import time
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime, timedelta

from tasks.download_ano_mes import download_csv_from_url
from tasks.upload_ano_mes import upload_csv_to_gcs

load_dotenv()

GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET_PROD")
BQ_TABLE = os.getenv("BQ_TABLE_EX_ORCAMENTARIA")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="exec_orcamentaria",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=True
)
def exec_orcamentaria_dag():
    @task()
    def download_task(**kwargs):
        logical_date = kwargs["logical_date"]
        download_csv_from_url(logical_date, conjunto="receitas_execucao", local_csv_path="/tmp/receitas_execucao_dadospb.csv")
        return "/tmp/receitas_execucao_dadospb.csv"

    @task()
    def upload_task(local_csv_path: str, **kwargs):
        logical_date = kwargs["logical_date"]

        time.sleep(30)
        return upload_csv_to_gcs(
            logical_date=logical_date,
            conjunto="receitas_execucao",
            local_csv_path=local_csv_path,
            gcp_bucket_name=GCP_BUCKET_NAME,
            gcp_conn_id="google_cloud_default"
        )

    @task()
    def carregar_para_bq_task(**kwargs):
        logical_date = kwargs["logical_date"]

        carregar_para_bq = GCSToBigQueryOperator(
            task_id="carregar_csv_para_bq",
            bucket=GCP_BUCKET_NAME,
            source_objects=[f"receitas_execucao/dadospb_{logical_date.year}_{logical_date.month:02d}.csv"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
            field_delimiter=";",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            gcp_conn_id="google_cloud_default"
        )
        
        carregar_para_bq.execute(kwargs)

    local_file_path = download_task()
    gcs_file_path = upload_task(local_file_path)
    carregar_para_bq_task().set_upstream(gcs_file_path)

exec_orcamentaria_dag()

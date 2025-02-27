from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime


def upload_csv_to_gcs(logical_date, conjunto, local_csv_path, gcp_bucket_name, gcp_conn_id):
    if isinstance(logical_date, str):
        logical_date = datetime.fromisoformat(logical_date)

    if logical_date.tzinfo is not None:
        logical_date = logical_date.astimezone().replace(tzinfo=None)

    year = logical_date.year
    object_name = f"{conjunto}/dadospb_{year}.csv"
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    gcs_hook.upload(
        bucket_name=gcp_bucket_name,
        object_name=object_name,
        filename=local_csv_path,
    )

    return f"gs://{gcp_bucket_name}/{object_name}"
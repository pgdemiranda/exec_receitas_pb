from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

def upload_csv_to_gcs(logical_date, conjunto, local_csv_path, gcp_bucket_name, gcp_conn_id):
    # Garantir que logical_date seja do tipo datetime
    if isinstance(logical_date, str):
        logical_date = datetime.fromisoformat(logical_date)

    if logical_date.tzinfo is not None:
        logical_date = logical_date.astimezone().replace(tzinfo=None)

    year = logical_date.year
    month = logical_date.month - 1

    if month == 0:
        month = 12
        year -= 1

    object_name = f"{conjunto}/dadospb_{year}_{month:02d}.csv"

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    gcs_hook.upload(
        bucket_name=gcp_bucket_name,
        object_name=object_name,
        filename=local_csv_path,
    )

    return f"gs://{gcp_bucket_name}/{object_name}"

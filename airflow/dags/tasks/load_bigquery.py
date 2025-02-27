from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def load_csv_to_bigquery(task_id, bucket_name, conjunto, dataset_table, gcp_conn_id, logical_date):
    """Carrega o CSV do GCS para o BigQuery."""
    year = logical_date.year
    month = logical_date.month

    source_object = f"{conjunto}/dadospb_{year}_{month:02d}.csv"

    return GCSToBigQueryOperator(
        task_id=task_id,
        bucket=bucket_name,
        source_objects=[source_object],
        destination_project_dataset_table=dataset_table,
        source_format="CSV",
        field_delimiter=";",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=gcp_conn_id,
    )

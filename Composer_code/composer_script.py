from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "ranjanrishi-project"
REGION = "us-central1"
CLUSTER_NAME = "my-cluster"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://rameshsamplebucket/gcs_to_bq_load_hcl_hackthon.py"
    },
}

with DAG(
    dag_id="dataproc_pyspark_submit",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gcp", "dataproc", "bigquery"],
) as dag:

    raw_layer_load = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    curated_layer_load = BigQueryInsertJobOperator(
    task_id="curated_layer_load",
    configuration={
        "query": {
            "query": "CALL `ranjanrishi-project.testdataset.sp_curated_layer_data_load`()",
            "useLegacySql": False,
        }
    },
    location="us-central1",
    )
    
    reporting_layer_load=BigQueryInsertJobOperator(
    task_id="reporting_layer_load",
    configuration={
        "query": {
            "query": "CALL `ranjanrishi-project.reporting.sp_kpi_report_load`()",
            "useLegacySql": False,
        }
    },
    location="us-central1",
    )

    raw_layer_load >> curated_layer_load >> reporting_layer_load
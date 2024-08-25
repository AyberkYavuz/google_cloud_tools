from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_dataflow_trigger',
    default_args=default_args,
    description='DAG to trigger Dataflow job for processing Parquet files in GCS',
    schedule_interval='30 8 * * *',  # Runs daily at 8:30 AM
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Step 1: List objects in the Prices folder to find the latest folder
    list_gcs_files = GCSListObjectsOperator(
        task_id='list_gcs_files',
        bucket='your-bucket-name',
        prefix='Prices/',
        gcp_conn_id='google_cloud_default',
    )

    # Step 2: Trigger Dataflow job using FlexTemplate
    trigger_dataflow = DataflowStartFlexTemplateOperator(
        task_id='start_dataflow_job',
        project_id='your-project-id',
        location='us-central1',  # Your Dataflow job region
        body={
            'launchParameter': {
                'jobName': 'process-parquet-daily-{{ ds_nodash }}',
                'containerSpecGcsPath': 'gs://your-bucket/templates/dataflow-template.json',
                'parameters': {
                    'input_file': 'gs://your-bucket/Prices/{{ ds_nodash }}/*.parquet',
                    'output_table': 'your-project-id:your_dataset.your_table',
                },
                'environment': {
                    'tempLocation': 'gs://your-bucket/temp',
                    'zone': 'us-central1-f'
                },
            }
        },
        gcp_conn_id='google_cloud_default',
        impersonation_chain=None,
    )

    # Set task dependencies
    list_gcs_files >> trigger_dataflow


# Daily Data Processing Pipeline with Apache Airflow and Google Cloud Dataflow

This repository contains an Apache Airflow DAG designed to trigger a Google Cloud Dataflow job that processes Parquet files uploaded daily to a Google Cloud Storage bucket. The processed data is then written to a BigQuery table for analysis.

## Table of Contents

- [Scenario](#scenario)
- [Architecture](#architecture)
- [Workflow Explanation](#workflow-explanation)
- [Airflow DAG](#airflow-dag)
- [Setup and Configuration](#setup-and-configuration)
  - [Prerequisites](#prerequisites)
  - [Google Cloud Setup](#google-cloud-setup)
  - [Airflow Setup](#airflow-setup)
- [Deployment](#deployment)
  - [Dataflow Template Creation](#dataflow-template-creation)
  - [Deploy the Airflow DAG](#deploy-the-airflow-dag)

## Scenario

A third-party application runs daily at 8:00 a.m., uploading Parquet files to a date-specific folder in a Google Cloud Storage (GCS) bucket named `Prices`. For example, files might be stored in `Prices/23082024/` (representing August 23, 2024). An Apache Airflow DAG runs every day at 8:30 a.m., detecting new date folders and triggering a Dataflow job to process these Parquet files and write the results to a BigQuery table.

## Architecture

The workflow includes the following components:

1. **Google Cloud Storage (GCS)**: A bucket (`Prices/`) where Parquet files are uploaded daily by a third-party application.
2. **Apache Airflow**: Orchestrates the workflow, scheduled to run daily at 8:30 a.m.
3. **Google Cloud Dataflow**: Processes the Parquet files using an Apache Beam pipeline.
4. **BigQuery**: Stores the processed data for analytics and reporting.

## Workflow Explanation

1. **Daily Upload of Parquet Files**:
   - At 8:00 a.m. each day, a third-party application uploads new Parquet files to a date-specific folder within the `Prices` folder in the GCS bucket (e.g., `Prices/23082024/`).

2. **Triggering the Airflow DAG**:
   - At 8:30 a.m., the Airflow scheduler triggers the `daily_dataflow_trigger` DAG. This DAG first checks the GCS bucket for new date folders that were created after the previous run.

3. **Listing GCS Files**:
   - The DAG uses the `GCSListObjectsOperator` to list all objects in the `Prices` folder. This step identifies the new date-specific folder that contains the Parquet files uploaded earlier that day.

4. **Starting the Dataflow Job**:
   - Once the new folder is identified, the DAG triggers a Dataflow job using the `DataflowStartFlexTemplateOperator`. The Dataflow job is based on a predefined template stored in GCS. It reads the Parquet files from the newly identified folder.

5. **Data Processing in Dataflow**:
   - The Dataflow job, using Apache Beam, processes the Parquet files. This might include data cleansing, transformation, and enrichment as per the business logic defined in the Beam pipeline (`dataflow_code.py`).

6. **Writing to BigQuery**:
   - After processing, the Dataflow job writes the transformed data to a specified BigQuery table. The table schema is either predefined or automatically detected if using schema autodetection.

7. **Completion and Monitoring**:
   - The Airflow DAG completes once the Dataflow job is successfully triggered. The Airflow UI provides monitoring capabilities to check the status of the DAG run and any tasks within it. Any failures in the DAG are retried as per the DAG configuration.

## Airflow DAG

Below is the Airflow DAG that automates this workflow:

```python
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
```

Airflow code also is given as a python file.

[airflow_dag_code.py](https://github.com/AyberkYavuz/google_cloud_tools/blob/main/airflow_dag_code.py)

## Setup and Configuration

### Prerequisites

Before setting up the Airflow DAG and Dataflow job, ensure you have the following:

* A Google Cloud Platform (GCP) account with the necessary permissions.
* A Google Cloud Storage (GCS) bucket to store Parquet files and Dataflow templates.
* A BigQuery dataset and table to store processed data.
* Apache Airflow installed and configured.


### Google Cloud Setup

1. **Create a GCS Bucket:** Create a bucket to store Parquet files and Dataflow templates.
2. **Create BigQuery Dataset and Table:** Define a dataset and table in BigQuery for storing processed data.
3. **Dataflow Template:** Package your Apache Beam pipeline and upload it as a Flex template to the GCS bucket.

### Airflow Setup

1. Install Apache Airflow and Google Cloud Provider:

```bash
pip install apache-airflow
pip install apache-airflow-providers-google
```

2. Configure Airflow Connections:

Set up a Google Cloud connection (google_cloud_default) in the Airflow Admin UI or use the command line to configure.

## Deployment

### Dataflow Template Creation

dataflow_code:

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    input_file = pipeline_options.view_as(PipelineOptions).input_file
    output_table = pipeline_options.view_as(PipelineOptions).output_table

    p = beam.Pipeline(options=pipeline_options)

    # Read Parquet files
    parquet_data = p | 'ReadParquet' >> beam.io.ReadFromParquet(input_file)

    # Perform some transformations (Example transformation: just passing through the data)
    transformed = (
        parquet_data
        | 'ExampleTransformation' >> beam.Map(lambda record: record)
    )

    # Write results to BigQuery
    transformed | 'WriteToBigQuery' >> WriteToBigQuery(
        output_table,
        schema='col1:STRING, col2:STRING',  # Adjust schema according to your Parquet files
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file',
        dest='input_file',
        required=True,
        help='Input file pattern for Parquet files in GCS.')
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output BigQuery table to write results to.')

    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args)
```

[dataflow_code.py](https://github.com/AyberkYavuz/google_cloud_tools/blob/main/dataflow_code.py)

To create and upload the Dataflow template:

```bash
python dataflow_code.py \
    --runner DataflowRunner \
    --project your-project-id \
    --staging_location gs://your-bucket/staging \
    --temp_location gs://your-bucket/temp \
    --template_location gs://your-bucket/templates/dataflow-template.json \
    --region us-central1
```

### Deploy the Airflow DAG

1. Start Airflow Services:

```bash
airflow webserver -p 8080
airflow scheduler
```

2. Activate the DAG:

Open the Airflow web UI, find the **daily_dataflow_trigger** DAG, and activate it.


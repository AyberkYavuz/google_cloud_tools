# Doc
This doc is for containing example scenario and source codes of that scenario. 
Google Cloud Functions and Dataflow usage is provided.

## Scenario
There is a main folder in Google Cloud Storage named Prices. And there is a 3rd party application that runs daily 
and add date folder under Prices folder. After that it loads parquet files in it. 
For example; Prices/23082024/ -> file1.parquet, file2. parquet , file3.parquet and file4.parquet

I will use Google Cloud Functions and Dataflow to get parquet files, do some operations and write them to BigQuery table, 
when new date folder and its files are created.

## Apache Beam Pipeline Code / Dataflow Code

First, let's define the Apache Beam pipeline that reads Parquet files and writes them to BigQuery:

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

The pipeline code also is given as a python file.

[dataflow_code.py](https://github.com/AyberkYavuz/google_cloud_tools/blob/main/dataflow_code.py)

## Packaging and Storing Apache Beam Template

To run the Apache Beam pipeline on Dataflow, package your Python script and dependencies, then store it as a template in Google Cloud Storage.

```bash
python dataflow_code.py \
    --runner DataflowRunner \
    --project your-project-id \
    --staging_location gs://your-bucket/staging \
    --temp_location gs://your-bucket/temp \
    --template_location gs://your-bucket/templates/dataflow-template.json \
    --region us-central1
```

## Google Cloud Function Code

Now, let's define the Google Cloud Function to trigger the Dataflow job.

```python
import os
from googleapiclient.discovery import build

# Set environment variables for project and Dataflow template location
PROJECT_ID = os.environ['GCP_PROJECT']
DATAFLOW_TEMPLATE = 'gs://your-bucket/templates/dataflow-template.json'
TEMP_LOCATION = 'gs://your-bucket/temp'


def trigger_dataflow(data, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket_name = data['bucket']
    file_name = data['name']

    # Check if the new file is a parquet file and is in a date folder
    if file_name.endswith('.parquet') and 'Prices/' in file_name:
        folder_name = file_name.split('/')[1]
        folder_path = f'gs://{bucket_name}/Prices/{folder_name}/*.parquet'

        # Build the Dataflow API service
        dataflow_service = build('dataflow', 'v1b3')
        request = dataflow_service.projects().locations().templates().launch(
            projectId=PROJECT_ID,
            location='us-central1',  # Update to your Dataflow job region
            body={
                'jobName': f'process-parquet-{folder_name}',
                'parameters': {
                    'input_file': folder_path,
                    'output_table': f'{PROJECT_ID}:your_dataset.your_table',
                },
                'environment': {
                    'tempLocation': TEMP_LOCATION
                }
            },
            gcsPath=DATAFLOW_TEMPLATE
        )
        response = request.execute()
        print(f'Dataflow job launched for {folder_name}: {response}')
```

## Deploying the Cloud Function

Deploy the Cloud Function using the gcloud command:

```python
gcloud functions deploy google_cloud_function_code \
    --runtime python39 \
    --trigger-resource YOUR_BUCKET_NAME \
    --trigger-event google.storage.object.finalize \
    --entry-point google_cloud_function_code \
    --set-env-vars GCP_PROJECT=your-project-id \
    --region us-central1
```

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

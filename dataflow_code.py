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

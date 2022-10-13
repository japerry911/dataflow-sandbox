import argparse
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


# noinspection PyUnresolvedReferences
def run():
    # Command line arguments
    parser = argparse.ArgumentParser(
        description="Load from Json into BigQuery"
    )
    parser.add_argument("--project", required=True,
                        help="Specify Google Cloud project")
    parser.add_argument("--region", required=True,
                        help="Specify Google Cloud region")
    parser.add_argument("--stagingLocation", required=True,
                        help="Specify Cloud Storage bucket for staging")
    parser.add_argument("--tempLocation", required=True,
                        help="Specify Cloud Storage bucket for temp")
    parser.add_argument("--runner", required=True,
                        help="Specify Apache Beam Runner")

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = "{0}{1}".format(
        "my-pipeline-", time.time_ns()
    )
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input_filename = "gs://dataflow-digible-poc-123/" \
                     "apartments__get_listings_rank-2022-10-11T11-01-30" \
                     ".parquet"
    dest_bq_landing_table = "{0}:land.apts_dot_com_ranking_landing".format(
        opts.project
    )
    dest_bq_trans_table = "{0}:transformed.apts_dot_com_ranking_transformed"\
        .format(opts.project)

    # Table schema for BigQuery
    landing_table_schema = {
        "fields": [
            {
                "name": "city",
                "type": "STRING"
            },
            {
                "name": "code",
                "type": "STRING"
            },
            {
                "name": "image",
                "type": "STRING"
            },
            {
                "name": "listings_url",
                "type": "string"
            },
            {
                "name": "package",
                "type": "STRING"
            },
            {
                "name": "package_rank",
                "type": "INTEGER"
            },
            {
                "name": "rank",
                "type": "INTEGER"
            },
            {
                "name": "rank_type",
                "type": "STRING"
            },
            {
                "name": "site",
                "type": "STRING"
            },
            {
                "name": "state",
                "type": "STRING"
            },
            {
                "name": "type",
                "type": "STRING"
            },
            {
                "name": "url",
                "type": "STRING"
            },
            {
                "name": "zip",
                "type": "STRING"
            }
        ]
    }

    def transform_city(line):
        line_dict = dict(line)
        line_dict["type"] = line_dict["type"].lower()
        return line_dict

    with beam.Pipeline(options=options) as p:
        raw_data = (p
                    | "ReadFromGCS" >> beam.io.ReadFromParquet(input_filename)
                    )

        (raw_data | "WriteDataToLanding" >> beam.io.WriteToBigQuery(
            dest_bq_landing_table,
            schema=landing_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        transformed_data = raw_data | beam.Map(transform_city)

        transformed_data | "WriteDataToBigQuery" >> beam.io.WriteToBigQuery(
            dest_bq_trans_table,
            schema=landing_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )


if __name__ == "__main__":
    run()

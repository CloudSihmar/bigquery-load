from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set the table ID to create.
table_id = "techlanders-internal.demo_data.employees"

# Define the schema for the table.
schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("dob", "DATE"),
    bigquery.SchemaField("addresses", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("zip", "STRING"),
        bigquery.SchemaField("numberOfYears", "INTEGER"),
    ]),
]

# Create the table if it does not exist.
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table, exists_ok=True)  # Make an API request.

# Load data from Cloud Storage into the table.
uri = "gs://sandeep-bucket44/employee.json"
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result()  # Wait for the job to complete.

print(f"Loaded {load_job.output_rows} rows into {table_id}.")

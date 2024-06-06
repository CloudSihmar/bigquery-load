from google.cloud import bigquery

# Define your project ID and dataset ID
project_id = "techlanders-internal"
dataset_id = "sandeepdataset"

# Define the table name and GCS URI
table_name = "user_data"
csv_uri = "gs://sandeep-bucket44/ibm-users.csv"

# Configure the external data source
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

# Define the schema for your data
schema = [
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("location", "STRING"),
]

# Create a BigQuery client
client = bigquery.Client()

# Create external table
table = bigquery.Table(dataset_ref.table(table_name), schema=schema)
external_config = bigquery.ExternalConfig("CSV")
external_config.source_uris = [csv_uri]
external_config.options.skip_leading_rows = 1
table.external_data_configuration = external_config

# Create a permanent table linked to the GCS file
table = client.create_table(table)

print(f"External table '{table.table_id}' created successfully!")

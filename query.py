from google.cloud import bigquery


# Define your project ID and dataset ID
project_id = "techlanders-internal"
dataset_id = "sandeepdataset"

# Define the table name
table_name = "user_data"

# Define the query
query = f"""
SELECT first_name, last_name
FROM `{project_id}.{dataset_id}.{table_name}`
WHERE location = 'Delhi'
"""

# Create a BigQuery client
client = bigquery.Client()

# Execute the query
query_job = client.query(query)  # Make an API request

# Wait for the query to complete
results = query_job.result()  # Waits for job to finish and retrieves results

# Print the results
for row in results:
  print(f"First Name: {row.first_name}, Last Name: {row.last_name}")

# Optional: Close the client 
client.close()

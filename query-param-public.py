from google.cloud import bigquery

# Create a BigQuery client
client = bigquery.Client()

# Define the query with parameters
query = """
SELECT
   country_name,
   EXTRACT(month FROM date) as month,
   EXTRACT(year FROM date) as year,
   SUM(cumulative_confirmed) AS cum_sum_cases
FROM
  `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE
  country_code = @country
AND date < @date
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3
"""

# Define the parameters
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("country", "STRING", "IN"),
        bigquery.ScalarQueryParameter("date", "DATE", "2022-01-01")
    ]
)

# Execute the query
query_job = client.query(query, job_config=job_config)

# Get the results
results = query_job.result()

# Print the results
for row in results:
    print(f"{row.country_name}, {row.month}, {row.year}, {row.cum_sum_cases}")

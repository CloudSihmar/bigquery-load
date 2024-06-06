#enable bigquery datatransfer in destination project
#enable billing of destination project
#pip install google-cloud-bigquery-datatransfer


# Import necessary libraries
from google.cloud import bigquery_datatransfer
import os

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/sandy_2087/sandeep-tech-425605-0946629299f8.json"

# Initialize the BigQuery Data Transfer Service client
transfer_client = bigquery_datatransfer.DataTransferServiceClient()

# Define your project and dataset IDs
destination_project_id = "sandeep-tech-425605"
destination_dataset_id = "destination_dataset"
source_project_id = "techlanders-internal"
source_dataset_id = "demo_dataset"

# Create the transfer configuration
transfer_config = bigquery_datatransfer.TransferConfig(
    destination_dataset_id=destination_dataset_id,
    display_name="copy_dataset",
    data_source_id="cross_region_copy",
    params={
        "source_project_id": source_project_id,
        "source_dataset_id": source_dataset_id,
    },
    schedule="every 24 hours",  # Set to a valid schedule string
)

# Create the transfer configuration on the destination project
transfer_config = transfer_client.create_transfer_config(
    parent=transfer_client.common_project_path(destination_project_id),
    transfer_config=transfer_config,
)

# Print the transfer configuration name
print(f"Created transfer config: {transfer_config.name}")

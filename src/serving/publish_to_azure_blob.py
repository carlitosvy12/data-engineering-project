from azure.storage.blob import BlobServiceClient
from pathlib import Path
import logging

# Simple logging configuration
logging.basicConfig(level=logging.INFO)


# Base project path
BASE_PATH = Path(__file__).resolve().parents[2]

# Path to the integrated CSV that will be uploaded
CSV_PATH = BASE_PATH / "data/staging/integrated/affordability_integrated.csv"

# Azure Storage account configuration

ACCOUNT_NAME = "deprojectstoragecarlos"
ACCOUNT_KEY = ""

CONTAINER_NAME = "integrated"



def upload_csv():
    # Build Azure connection string
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )

    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)


    # Get reference to the target blob
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME,
        blob="affordability_integrated.csv"
    )


    # Upload the CSV file (overwrite if it already exists)
    with open(CSV_PATH, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    logging.info("CSV uploaded to Azure Blob Storage")

if __name__ == "__main__":
    # Run upload when script is executed directly
    upload_csv()

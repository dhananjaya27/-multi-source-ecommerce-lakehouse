import os
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

local_file_path = "src/config/data_quality_rules.csv"
adls_directory_path = "raw/config"
adls_file_name = "data_quality_rules.csv"

account_url = f"https://{storage_account_name}.dfs.core.windows.net"

service_client = DataLakeServiceClient(
    account_url=account_url,
    credential=storage_account_key
)

file_system_client = service_client.get_file_system_client(container_name)

directory_client = file_system_client.get_directory_client(adls_directory_path)

try:
    directory_client.create_directory()
except Exception:
    pass

file_client = directory_client.get_file_client(adls_file_name)

with open(local_file_path, "rb") as file:
    file_client.upload_data(file, overwrite=True)

print(f"Uploaded {local_file_path} to ADLS path {adls_directory_path}/{adls_file_name}")
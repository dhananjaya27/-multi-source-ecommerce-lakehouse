import csv
import os
from datetime import datetime
from pathlib import Path

from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv


# Load Azure values from .env file
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

LOCAL_RAW_FOLDER = "data/raw"
ADLS_RAW_FOLDER = "raw"

AUDIT_LOG_FOLDER = "logs"
AUDIT_LOG_FILE = "logs/adls_upload_audit_log.csv"


def create_adls_client():
    """
    Create ADLS Gen2 client using storage account access key.
    """

    if not STORAGE_ACCOUNT_NAME:
        raise ValueError("AZURE_STORAGE_ACCOUNT_NAME is missing in .env file")

    if not STORAGE_ACCOUNT_KEY:
        raise ValueError("AZURE_STORAGE_ACCOUNT_KEY is missing in .env file")

    if not CONTAINER_NAME:
        raise ValueError("AZURE_CONTAINER_NAME is missing in .env file")

    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

    service_client = DataLakeServiceClient(
        account_url=account_url,
        credential=STORAGE_ACCOUNT_KEY,
    )

    file_system_client = service_client.get_file_system_client(CONTAINER_NAME)

    return file_system_client


def write_audit_log(source_file, target_adls_path, status, error_message=""):
    """
    Write one upload status record into local audit log CSV.
    """

    os.makedirs(AUDIT_LOG_FOLDER, exist_ok=True)

    audit_file_exists = os.path.exists(AUDIT_LOG_FILE)

    with open(AUDIT_LOG_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        if not audit_file_exists:
            writer.writerow(
                [
                    "source_file",
                    "target_adls_path",
                    "status",
                    "upload_timestamp",
                    "error_message",
                ]
            )

        writer.writerow(
            [
                source_file,
                target_adls_path,
                status,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                error_message,
            ]
        )


def upload_one_file(file_system_client, local_file_path):
    """
    Upload one local file to ADLS and write audit status.
    """

    relative_path = os.path.relpath(local_file_path, LOCAL_RAW_FOLDER)

    adls_file_path = os.path.join(ADLS_RAW_FOLDER, relative_path).replace("\\", "/")

    directory_path = str(Path(adls_file_path).parent).replace("\\", "/")
    file_name = Path(adls_file_path).name

    try:
        directory_client = file_system_client.get_directory_client(directory_path)

        try:
            directory_client.create_directory()
        except Exception:
            # Directory may already exist. That is okay.
            pass

        file_client = directory_client.get_file_client(file_name)

        with open(local_file_path, "rb") as local_file:
            file_client.upload_data(local_file, overwrite=True)

        print(f"Uploaded: {local_file_path} -> {adls_file_path}")

        write_audit_log(
            source_file=local_file_path,
            target_adls_path=adls_file_path,
            status="SUCCESS",
        )

    except Exception as error:
        print(f"Failed: {local_file_path} -> {adls_file_path}")
        print(f"Error: {error}")

        write_audit_log(
            source_file=local_file_path,
            target_adls_path=adls_file_path,
            status="FAILED",
            error_message=str(error),
        )


def upload_raw_folder():
    """
    Upload all files from local data/raw folder to ADLS raw folder.
    """

    if not os.path.exists(LOCAL_RAW_FOLDER):
        raise FileNotFoundError(f"Local raw folder not found: {LOCAL_RAW_FOLDER}")

    file_system_client = create_adls_client()

    for root, _, files in os.walk(LOCAL_RAW_FOLDER):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            upload_one_file(file_system_client, local_file_path)

    print("Upload process completed.")
    print(f"Audit log created at: {AUDIT_LOG_FILE}")


if __name__ == "__main__":
    upload_raw_folder()
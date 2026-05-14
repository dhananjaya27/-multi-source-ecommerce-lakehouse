import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.utils.config_loader import load_config


CONFIG_PATH = "src/config/pipeline_config.yml"


def get_enabled_api_sources(config: dict) -> dict:
    """
    Get only enabled API sources from pipeline config.
    """

    api_sources = {}

    sources = config.get("sources", {})

    for source_name, source_config in sources.items():
        if (
            source_config.get("enabled", False)
            and source_config.get("source_type") == "api"
        ):
            api_sources[source_name] = source_config

    return api_sources


def create_raw_file_name(source_name: str) -> str:
    """
    Create a unique raw JSON file name using source name and timestamp.
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return f"{source_name}_{timestamp}.json"


def fetch_api_data(api_url: str) -> dict:
    """
    Fetch data from an API endpoint.
    """

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    return response.json()


def write_api_data_to_raw(source_name: str, source_config: dict) -> None:
    """
    Fetch API data and write it into raw landing folder.
    """

    api_url = source_config["api_url"]
    raw_path = source_config["raw_path"]

    os.makedirs(raw_path, exist_ok=True)

    data = fetch_api_data(api_url)

    raw_file_name = create_raw_file_name(source_name)
    target_path = os.path.join(raw_path, raw_file_name)

    with open(target_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, default=str)

    print(f"Fetched {api_url} → {target_path}")


def run_api_ingestion() -> None:
    """
    Main function to ingest all enabled API sources.
    """

    config = load_config(CONFIG_PATH)

    api_sources = get_enabled_api_sources(config)

    if not api_sources:
        print("No enabled API sources found in config.")
        return

    print(f"Found API sources: {list(api_sources.keys())}")

    for source_name, source_config in api_sources.items():
        write_api_data_to_raw(source_name, source_config)

    print("API ingestion completed successfully.")


if __name__ == "__main__":
    run_api_ingestion()
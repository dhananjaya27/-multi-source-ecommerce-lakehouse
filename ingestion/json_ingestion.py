import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.utils.config_loader import load_config


CONFIG_PATH = "src/config/pipeline_config.yml"


def get_enabled_json_sources(config: dict) -> dict:
    """
    Get only enabled JSON sources from pipeline config.
    """

    json_sources = {}

    sources = config.get("sources", {})

    for source_name, source_config in sources.items():
        if (
            source_config.get("enabled", False)
            and source_config.get("source_type") == "json"
        ):
            json_sources[source_name] = source_config

    return json_sources


def create_raw_file_name(source_name: str) -> str:
    """
    Create a unique raw JSON file name using source name and timestamp.
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return f"{source_name}_{timestamp}.json"


def copy_json_to_raw(source_name: str, source_config: dict) -> None:
    """
    Copy one JSON source file into its raw landing folder.
    """

    source_path = source_config["source_path"]
    raw_path = source_config["raw_path"]

    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source JSON file not found: {source_path}")

    os.makedirs(raw_path, exist_ok=True)

    raw_file_name = create_raw_file_name(source_name)
    target_path = os.path.join(raw_path, raw_file_name)

    shutil.copy2(source_path, target_path)

    print(f"Copied {source_path} → {target_path}")


def run_json_ingestion() -> None:
    """
    Main function to ingest all enabled JSON sources.
    """

    config = load_config(CONFIG_PATH)

    json_sources = get_enabled_json_sources(config)

    if not json_sources:
        print("No enabled JSON sources found in config.")
        return

    print(f"Found JSON sources: {list(json_sources.keys())}")

    for source_name, source_config in json_sources.items():
        copy_json_to_raw(source_name, source_config)

    print("JSON ingestion completed successfully.")


if __name__ == "__main__":
    run_json_ingestion()
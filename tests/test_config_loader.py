import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.utils.config_loader import load_config

config = load_config("src/config/pipeline_config.yml")

print(config)
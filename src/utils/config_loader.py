#read file from pipeline_config.yaml

import yaml
from pathlib import Path

def load_config(config_path:str)->dict:
    path=Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}") 

    with open(path, 'r') as file:
        config= yaml.safe_load(file)
        
    if not config:
        raise ValueError(f"Config file at {config_path} is empty or invalid") 
    return config  
        
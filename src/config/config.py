import logging
import yaml
import os
import re
from dotenv import load_dotenv
from pydantic import BaseModel

# Load .env from the working directory
load_dotenv()
CONFIG_FILE_PATH = "config.yaml"

class FetcherConfig(BaseModel):
    tushare_token: str

class Configs(BaseModel):
    fetcher: FetcherConfig

def load_config(path: str = CONFIG_FILE_PATH) -> Configs:
    """
    Load .env variables and app configs
    Args:
        path: the config.yaml file path, relative to the root directory
    Returns:
        the loaded Configs object
    """
    with open(path, encoding="utf-8") as config_file:
        content = config_file.read()
    pattern = re.compile(r"\$\{(\w+)\}")
    content = pattern.sub(lambda match: os.getenv(match.group(1), ""), content)
    data = yaml.safe_load(content)
    return Configs(**data)

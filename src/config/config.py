import os
import re

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel

# Load .env from the working directory
load_dotenv()
CONFIG_FILE_PATH = "config.yaml"


class FetcherConfig(BaseModel):
    tushare_token: str


class StoreConfig(BaseModel):
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    bucket_endpoint: str
    runtime_env: str


class MetaConfig(BaseModel):
    meta_collection_name: str


class Configs(BaseModel):
    fetcher: FetcherConfig
    store: StoreConfig
    meta: MetaConfig


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

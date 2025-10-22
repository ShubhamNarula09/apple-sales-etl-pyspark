import os
from pyspark.sql import DataFrame
from loguru import logger

def check_file_exists(path: str):
    if not os.path.exists(path):
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")
    return True

def validate_dataframe(df: DataFrame, required_columns: list):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
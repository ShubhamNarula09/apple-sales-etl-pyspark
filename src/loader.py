from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from loguru import logger
from pyspark.sql.functions import broadcast
from .reader_factory import get_data_source
from .spark_manager import SparkManager
import os


class Loader(ABC):
    """Abstract base class for all data loaders."""

    def __init__(self):
        self.spark: SparkSession = SparkManager.get_spark()

    @abstractmethod
    def load(self) -> DataFrame:
        """Load and return a Spark DataFrame."""
        raise NotImplementedError(f"Function {self.__class__.__name__}.load not implemented")


# File Loader Subclass for Loading Data

class FileLoader(Loader):
    """File-based loader supporting broadcast, partitioning, and bucketing"""

    def __init__(self, file_path: str, data_type: str, broadcast_table: bool):
        super().__init__()
        self.file_path = file_path
        self.data_type = data_type
        self.broadcast_table = broadcast_table

    def load(self) -> DataFrame:
        """Load DataFrame from file with optional broadcast"""
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            raise FileNotFoundError(f"File not found: {self.file_path}")

        df = get_data_source(self.data_type, self.file_path).get_dataframe()

        if self.broadcast_table:
            df = broadcast(df)
            logger.info(f"Broadcast applied to {self.file_path}")

        return df

    def write(self, df: DataFrame, path: str, file_format: str = "csv", partition_by: list = None, bucket_by: list = None, num_buckets: int = None, mode: str = "overwrite"):
        """Write DataFrame with optional partitioning and bucketing"""
        writer = df.write.format(file_format).mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)
            logger.info(f"Partitioning by: {partition_by}")

        if bucket_by and num_buckets:
            writer = writer.bucketBy(num_buckets, *bucket_by)
            logger.info(f"Bucketing by: {bucket_by} into {num_buckets} buckets")

        writer.save(path)
        logger.info(f"Data saved to {path} with format={file_format}")

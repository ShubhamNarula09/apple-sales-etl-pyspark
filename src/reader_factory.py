from abc import ABC, abstractmethod
from loguru import logger
import inspect
from .spark_manager import SparkManager

# -------------------------
# Abstract base class
# -------------------------

class DataSource(ABC):
    """Abstract class for all data sources."""
    def __init__(self, path):
        self.path = path
        self.spark = SparkManager.get_spark()

    @abstractmethod
    def get_dataframe(self):
        """Abstract method to get dataframe."""
        raise NotImplementedError(f"Function {inspect.currentframe().f_code.co_name} not implemented")

# -------------------------
# File-based data source
# -------------------------
class FileDataSource(DataSource):
    """Generic file data source for CSV, Parquet, Delta."""
    def __init__(self, path, file_format):
        super().__init__(path)
        self.file_format = file_format

    def get_dataframe(self):
        if not self.path:
            logger.error(f"Path argument missing in function {inspect.currentframe().f_code.co_name}")
            raise ValueError(f"Path not provided in {inspect.currentframe().f_code.co_name}")

        try:
            return (
                self.spark
                .read
                .format(self.file_format)
                .option("header", True)
                .load(self.path)
            )
        except Exception as e:
            logger.exception(f"Error reading {self.file_format} file: {e}")
            raise

# -------------------------
# Factory function
# -------------------------
def get_data_source(data_type, file_path):
    """
    Factory function to return a DataSource instance based on data_type.
    Returns:
        DataSource: FileDataSource with the correct format.
    Raises:
        ValueError: If the data_type is invalid.
    """
    format_mapping = {
        "csv": "csv",
        "parquet": "parquet",
        "delta": "delta"
    }

    file_format = format_mapping.get(data_type.lower())
    if not file_format:
        raise ValueError(f"Invalid data type: {data_type} in {inspect.currentframe().f_code.co_name}")
    return FileDataSource(file_path, file_format)
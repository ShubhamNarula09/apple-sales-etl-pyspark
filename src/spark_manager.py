from pyspark.sql import SparkSession
import configparser
import os
from loguru import logger

config_path = os.getenv("CONFIG_PATH", "config/config.ini")
if not os.path.exists(config_path):
    logger.error(f"Configuration file not found: {config_path}")
    raise FileNotFoundError(f"Config file not found: {config_path}")

config = configparser.ConfigParser()
config.read(config_path)

class SparkManager:
    """Singleton SparkSession manager."""
    _spark = None

    @classmethod
    def get_spark(self):
        appName = config.get(section="sparkConfigurations", option="appName")
        master = config.get(section="sparkConfigurations", option="master")

        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(appName)
                .master(master)  # Change to cluster master in production
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("ERROR")
        return self._spark

    @classmethod
    def stop_spark(self):
        if self._spark:
            self._spark.stop()
            self._spark = None

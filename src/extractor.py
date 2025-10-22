from abc import ABC, abstractmethod
from loguru import logger
from pyspark.sql import DataFrame
import os
from src.reader_factory import get_data_source
from src.helper import check_file_exists
import configparser

import os
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_PATH", "config/config.ini"))

class Extractor(ABC):
    @abstractmethod
    def extract(self) -> dict:
        pass

class TransactionCustomerExtractor(Extractor):
    """Extractor for transactions and customers."""
    def extract(self):
        transaction_csv = config.get("AppleAnalysisCSV", "transactions_df_path")
        customer_csv = config.get("AppleAnalysisCSV", "customers_df_path")
        check_file_exists(transaction_csv)
        check_file_exists(customer_csv)

        transactionDF: DataFrame = get_data_source("csv", transaction_csv).get_dataframe()
        customerDF: DataFrame = get_data_source("csv", customer_csv).get_dataframe()
        return {"transactionInputDF": transactionDF, "customerInputDF": customerDF}

class ProductExtractor(Extractor):
    """Extractor for products."""
    def extract(self):
        products_csv = config.get("AppleAnalysisCSV", "products_df_path")
        check_file_exists(products_csv)
        productDF = get_data_source("csv", products_csv).get_dataframe()
        return {"productInputDF": productDF}

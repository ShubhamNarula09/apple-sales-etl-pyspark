from loguru import logger
from src.loader import FileLoader
from src.transformer import HighValueCustomersTransformer
from configparser import ConfigParser
import os

# Load configuration

config_path = os.getenv("CONFIG_PATH", "config/config.ini")
if not os.path.exists(config_path):
    logger.error(f"Configuration file not found: {config_path}")
    raise FileNotFoundError(f"Config file not found: {config_path}")

config = ConfigParser()
config.read(config_path)

OUTPUT_PATH = config.get("OutputPath", "output_path")

class HighValueCustomersWorkflow:
    """Workflow to identify high-spending customers and write partitioned output."""

    def runner(self):
        try:
            # Load DataFrames using Loader
            transaction_loader = FileLoader(
                file_path=config.get("AppleAnalysisCSV", "transactions_df_path"),
                data_type="csv"
            )
            transaction_df = transaction_loader.load()

            customer_loader = FileLoader(
                file_path=config.get("AppleAnalysisCSV", "customers_df_path"),
                data_type="csv",
                broadcast_table=True
            )
            customer_df = customer_loader.load()

            product_loader = FileLoader(
                file_path=config.get("AppleAnalysisCSV", "products_df_path"),
                data_type="csv",
                broadcast_table=True
            )
            product_df = product_loader.load()

             # Prepare input DataFrames for transformation
            input_dfs = {
                "transactionInputDF": transaction_df.select("customer_id", "product_name"),
                "customerInputDF": customer_df.select("customer_id", "customer_name", "location"),
                "productInputDF": product_df.select("product_name", "price")
            }

            # Transform
            result_df = HighValueCustomersTransformer().transform(input_dfs)

            # Ensure location exists for partitioning
            if "location" not in result_df.columns:
                result_df = result_df.join(
                    customer_df.select("customer_id", "location"),
                    on="customer_id",
                    how="left"
                )

            # Write output partitioned by location
            transaction_loader.write(df=result_df, path=OUTPUT_PATH, file_format="parquet", partition_by=["location"])
            logger.info(f"HighValueCustomersWorkflow completed and saved to {OUTPUT_PATH}")

        except Exception as e:
            logger.exception(f"HighValueCustomersWorkflow failed with error {e}")
            raise e
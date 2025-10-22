from loguru import logger
from pyspark.sql.functions import broadcast
from src.extractor import TransactionCustomerExtractor
from src.transformer import IphoneBeforeAirpodsTransformer


class IphoneBeforeAirpodsWorkflow:
    """Find customers who bought iPhone before AirPods implementing broadcast join."""

    def runner(self):
        try:
            # Extract transaction and customer data
            dataframes = TransactionCustomerExtractor().extract()

            # Broadcast join: customer_df is very small (~100 rows)
            dataframes["customerInputDF"] = broadcast(dataframes["customerInputDF"])

            # Transform the data
            df = IphoneBeforeAirpodsTransformer().transform(dataframes)

            # Show final result
            logger.info("IphoneBeforeAirpodsWorkflow completed")
            df.show()
        except Exception as e:
            logger.exception("Workflow failed")
            raise e

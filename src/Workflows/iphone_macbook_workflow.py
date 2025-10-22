from loguru import logger
from pyspark.sql.functions import broadcast
from src.extractor import TransactionCustomerExtractor
from src.transformer import IphoneMacbookTransformer


class IphoneMacbookWorkflow:
    """Find customers who bought both iPhone and MacBook ."""

    def runner(self):
        try:
            dataframes = TransactionCustomerExtractor().extract()
            # Broadcast join: customer_df is small
            dataframes["customerInputDF"] = broadcast(dataframes["customerInputDF"])

            # Column pruning: keep only necessary columns before transformation
            dataframes["transactionInputDF"] = dataframes["transactionInputDF"].select("customer_id", "product_name")
            # Transform
            df = IphoneMacbookTransformer().transform(dataframes)

            logger.info("IphoneMacbookWorkflow completed")
            df.show()

        except Exception as e:
            logger.exception("Workflow failed")
            raise e

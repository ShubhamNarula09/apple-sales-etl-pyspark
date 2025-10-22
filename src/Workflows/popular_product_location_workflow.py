from src.extractor import TransactionCustomerExtractor
from src.transformer import PopularProductLocationTransformer
from loguru import logger

class PopularProductLocationWorkflow:
    def runner(self):
        try:
            dataframes = TransactionCustomerExtractor().extract()
            df = PopularProductLocationTransformer().transform(dataframes)
            logger.info("PopularProductLocationWorkflow completed")
            df.show()
        except Exception as e:
            logger.exception("Workflow failed")
            raise e

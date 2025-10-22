from loguru import logger

from src.spark_manager import SparkManager
from src.Workflows.iphone_airpods_workflow import IphoneBeforeAirpodsWorkflow
from src.Workflows.iphone_macbook_workflow import IphoneMacbookWorkflow
from src.Workflows.high_value_customers_workflow import HighValueCustomersWorkflow
from src.Workflows.popular_product_location_workflow import PopularProductLocationWorkflow


if __name__ == "__main__":
    spark = SparkManager.get_spark()
    try:
        workflows = [
            IphoneBeforeAirpodsWorkflow(),
            IphoneMacbookWorkflow(),
            HighValueCustomersWorkflow(),
            PopularProductLocationWorkflow()
        ]

        for wf in workflows:
            wf.runner()
    finally:
        logger.info("Stopping Spark session")
        SparkManager.stop_spark()

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.transformer import HighValueCustomersTransformer

class HighValueCustomersTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("TestHighValueCustomers") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_high_value_customers_transformer(self):
        # Create mock input DataFrames

        transaction_data = [
            Row(customer_id="105", product_name="iPhone"),
            Row(customer_id="105", product_name="AirPods"),
            Row(customer_id="106", product_name="MacBook")
        ]
        customer_data = [
            Row(customer_id="105", customer_name="Eva", location="Ohio"),
            Row(customer_id="106", customer_name="Frank", location="Nevada")
        ]

        transaction_df = self.spark.createDataFrame(transaction_data)
        customer_df = self.spark.createDataFrame(customer_data)

        input_dfs = {
            "transactionInputDF": transaction_df,
            "customerInputDF": customer_df
        }

        # Run transformer
        result_df = HighValueCustomersTransformer().transform(input_dfs)

        # Check results
        result = [row.asDict() for row in result_df.collect()]

        expected = [
            {"customer_id": "105", "customer_name": "Eva", "total_spent": 0}  # example expected row
        ]

        self.assertTrue(len(result) > 0)  # At least one result
        self.assertIn("customer_name", result[0])  # Column exists

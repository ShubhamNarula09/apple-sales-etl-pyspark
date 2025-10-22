from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead, collect_set, array_contains, sum as spark_sum, desc, count, rank
from pyspark.sql.functions import broadcast

class Transformer(ABC):
    @abstractmethod
    def transform(self, dataframes: dict) -> DataFrame:
        pass

class IphoneBeforeAirpodsTransformer(Transformer):
    def transform(self, dataframes: dict):
        transaction_df = dataframes["transactionInputDF"]
        customer_df = dataframes["customerInputDF"]

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transaction_df = transaction_df.withColumn("next_product_name", lead("product_name").over(windowSpec))
        filtered_df = transaction_df.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        ).select("transaction_id", "customer_id")
        joined_df = customer_df.join(broadcast(filtered_df), "customer_id").select("transaction_id", "customer_name")
        return joined_df

class IphoneMacbookTransformer(Transformer):
    def transform(self, dataframes: dict):
        transaction_df = dataframes["transactionInputDF"]
        customer_df = dataframes["customerInputDF"]

        purchased = transaction_df.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        filtered = purchased.filter(array_contains(col("products"), "iPhone") & array_contains(col("products"), "MacBook"))
        return filtered.join(customer_df, "customer_id").select("customer_id", "customer_name", "products")

class HighValueCustomersTransformer(Transformer):
    def transform(self, dataframes: dict):
        transaction_df = dataframes["transactionInputDF"]
        customer_df = dataframes["customerInputDF"]
        product_df = dataframes["productInputDF"]

        joined = transaction_df.join(product_df, transaction_df.product_name == product_df.product_name, "inner")
        aggregated = joined.groupBy("customer_id").agg(spark_sum("price").alias("total_spent"))
        filtered = aggregated.filter(col("total_spent") > 1000)
        return filtered.join(customer_df, "customer_id").select("customer_id", "customer_name", "total_spent")

class PopularProductLocationTransformer(Transformer):
    def transform(self, dataframes: dict):
        transaction_df = dataframes["transactionInputDF"]
        customer_df = dataframes["customerInputDF"]

        joined = transaction_df.join(customer_df, "customer_id")
        agg_df = joined.groupBy("location", "product_name").agg(count("*").alias("count"))
        windowSpec = Window.partitionBy("location").orderBy(desc("count"))
        ranked = agg_df.withColumn("rank", rank().over(windowSpec)).filter(col("rank") == 1).drop("rank")
        return ranked.select("location", "product_name", "count")

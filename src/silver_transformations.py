import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def transform_to_silver(spark: SparkSession, bronze_path: str, silver_path: str):
    print("--- Starting Silver Transformations ---")

    try:
        # 1. Orders: Keep only shipped/delivered, cast strings to Timestamps
        print("Transforming orders...")
        orders = spark.read.parquet(f"{bronze_path}/orders")
        orders_clean = orders.filter(col("order_status").isin("delivered", "shipped")) \
            .dropDuplicates(["order_id"]) \
            .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
            .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
            .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
        orders_clean.write.mode("overwrite").parquet(f"{silver_path}/orders")

        # 2. Order Items: Deduplicate and ensure valid pricing
        print("Transforming order_items...")
        items = spark.read.parquet(f"{bronze_path}/order_items")
        items_clean = items.dropDuplicates(["order_id", "order_item_id"]) \
            .filter(col("price") > 0)
        items_clean.write.mode("overwrite").parquet(f"{silver_path}/order_items")

        # 3. Sellers: Deduplicate
        print("Transforming sellers...")
        sellers = spark.read.parquet(f"{bronze_path}/sellers")
        sellers_clean = sellers.dropDuplicates(["seller_id"])
        sellers_clean.write.mode("overwrite").parquet(f"{silver_path}/sellers")

        # 4. Reviews: Deduplicate and impute missing review scores
        print("Transforming order_reviews...")
        reviews = spark.read.parquet(f"{bronze_path}/order_reviews")
        reviews_clean = reviews.dropDuplicates(["review_id"]) \
            .na.fill({"review_score": 3})
        reviews_clean.write.mode("overwrite").parquet(f"{silver_path}/order_reviews")

        print("--- Silver Transformations Complete ---")

    except Exception as e:
        print(f"Silver transformation failed. Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("OlistSilverTransformation").getOrCreate()
    transform_to_silver(spark, "/opt/airflow/data/bronze", "/opt/airflow/data/silver")
    spark.stop()
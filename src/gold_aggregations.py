import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, when, avg, sum, count


def create_gold_seller_metrics(spark: SparkSession, silver_path: str, gold_path: str):
    print("--- Starting Gold Aggregations ---")

    try:
        # Enable Adaptive Query Execution for performance optimization
        spark.conf.set("spark.sql.adaptive.enabled", "true")

        # Read Cleaned Silver Data
        orders = spark.read.parquet(f"{silver_path}/orders")
        items = spark.read.parquet(f"{silver_path}/order_items")
        reviews = spark.read.parquet(f"{silver_path}/order_reviews")
        sellers = spark.read.parquet(f"{silver_path}/sellers")

        # Feature Engineering: Flag late deliveries
        orders_enriched = orders.withColumn(
            "is_late",
            when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0)
        )

        # OPTIMIZATION: Broadcast Join the small sellers table to prevent shuffle operations
        heavy_join = items.join(orders_enriched, "order_id", "inner") \
            .join(reviews, "order_id", "left")

        final_df = heavy_join.join(broadcast(sellers), "seller_id", "inner")

        # Aggregate: Calculate metrics per seller
        seller_metrics = final_df.groupBy("seller_id", "seller_city", "seller_state").agg(
            sum("price").alias("total_revenue"),
            avg("review_score").alias("avg_review_score"),
            avg("is_late").alias("late_delivery_rate"),
            count("order_id").alias("total_orders")
        )

        # OPTIMIZATION: Repartition before writing to prevent "small file syndrome"
        seller_metrics.repartition(4).write.mode("overwrite").parquet(f"{gold_path}/seller_metrics")
        print("--- Gold Aggregations Complete ---")

    except Exception as e:
        print(f"Gold aggregation failed. Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("OlistGoldAggregation").getOrCreate()
    create_gold_seller_metrics(spark, "/opt/airflow/data/silver", "/opt/airflow/data/gold")
    spark.stop()
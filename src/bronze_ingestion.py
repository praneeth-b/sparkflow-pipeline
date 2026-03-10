import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def ingest_to_bronze(spark: SparkSession, raw_path: str, bronze_path: str):
    print("--- Starting Bronze Ingestion ---")

    # Define Strict Schemas to prevent pipeline corruption from bad raw_csv files
    schemas = {
        "orders": StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", StringType(), True),
            StructField("order_approved_at", StringType(), True),
            StructField("order_delivered_carrier_date", StringType(), True),
            StructField("order_delivered_customer_date", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True)
        ]),
        "order_items": StructType([
            StructField("order_id", StringType(), False),
            StructField("order_item_id", IntegerType(), False),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("freight_value", FloatType(), True)
        ]),
        "order_reviews": StructType([
            StructField("review_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("review_score", IntegerType(), True),
            StructField("review_comment_title", StringType(), True),
            StructField("review_comment_message", StringType(), True),
            StructField("review_creation_date", StringType(), True),
            StructField("review_answer_timestamp", StringType(), True)
        ]),
        "sellers": StructType([
            StructField("seller_id", StringType(), False),
            StructField("seller_zip_code_prefix", IntegerType(), True),
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True)
        ])
    }

    # Iterate, enforce schema, and write to Parquet
    for table_name, schema in schemas.items():
        print(f"Ingesting {table_name}...")
        try:
            df = spark.read.csv(
                f"{raw_path}/olist_{table_name}_dataset.csv",
                header=True,
                schema=schema,
                mode="DROPMALFORMED"  # Drop rows violating the strict schema
            )
            df.write.mode("overwrite").parquet(f"{bronze_path}/{table_name}")
            print(f"Successfully wrote {table_name} to Bronze.")

        except Exception as e:
            print(f"Failed to ingest {table_name}. Error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("OlistBronzeIngestion").getOrCreate()
    # Paths point to the mounted volumes inside the Docker container
    ingest_to_bronze(spark, "/opt/airflow/data/raw_csv", "/opt/airflow/data/bronze")
    spark.stop()
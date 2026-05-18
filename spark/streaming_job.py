"""
╔══════════════════════════════════════════════════════════════════╗
║   E-COMMERCE ANALYTICS PLATFORM — SPARK STRUCTURED STREAMING    ║
║   Phase 2: Reads from Kafka → Cleans → Writes to Delta Lake     ║
║                                                                  ║
║   Topics:  orders | payments | user_events                       ║
║   Output:  /delta-lake/raw/     (exact copy from Kafka)          ║
║            /delta-lake/curated/ (cleaned + enriched)             ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ── SECTION 2: IMPORTS ────────────────────────────────────────────
import os
import json
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop;" + os.environ.get("PATH", "")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when,
    lit, upper, trim, round as spark_round,
    current_timestamp, dayofweek, hour,
    regexp_replace, length
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    BooleanType, ArrayType, LongType
)

# ── WINDOWS FIX ───────────────────────────────────────────────────




# ── SECTION 3: PATHS CONFIGURATION ───────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
BASE_PATH    = os.path.join(PROJECT_ROOT, "delta-lake")

RAW_ORDERS    = f"{BASE_PATH}/raw/orders"
RAW_PAYMENTS  = f"{BASE_PATH}/raw/payments"
RAW_EVENTS    = f"{BASE_PATH}/raw/user_events"

CUR_ORDERS    = f"{BASE_PATH}/curated/orders"
CUR_PAYMENTS  = f"{BASE_PATH}/curated/payments"
CUR_EVENTS    = f"{BASE_PATH}/curated/user_events"

CHK_ORDERS    = f"{BASE_PATH}/checkpoints/orders"
CHK_PAYMENTS  = f"{BASE_PATH}/checkpoints/payments"
CHK_EVENTS    = f"{BASE_PATH}/checkpoints/user_events"

KAFKA_SERVERS = "localhost:9092"


# ── SECTION 4: CREATE DIRECTORIES ────────────────────────────────
def create_directories():
    dirs = [
        RAW_ORDERS, RAW_PAYMENTS, RAW_EVENTS,
        CUR_ORDERS, CUR_PAYMENTS, CUR_EVENTS,
        CHK_ORDERS, CHK_PAYMENTS, CHK_EVENTS,
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
    print("✅ All directories created")


# ── SECTION 5: CREATE SPARK SESSION ──────────────────────────────
def create_spark_session():
    spark = SparkSession.builder \
        .appName("EcommerceStreamingPipeline") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-core_2.12:2.4.0",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
            ])
        ) \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession created successfully")
    print(f"   Spark Version: {spark.version}")
    print(f"   Spark UI:      http://localhost:4040")
    return spark


# ── SECTION 6: ORDER SCHEMA ───────────────────────────────────────
def get_order_schema():
    return StructType([
        StructField("order_id",       StringType(),  nullable=True),
        StructField("customer_id",    StringType(),  nullable=True),
        StructField("customer_name",  StringType(),  nullable=True),
        StructField("customer_email", StringType(),  nullable=True),
        StructField("customer_phone", StringType(),  nullable=True),
        StructField("region",         StringType(),  nullable=True),
        StructField("city",           StringType(),  nullable=True),
        StructField("address",        StringType(),  nullable=True),
        StructField("num_items",      IntegerType(), nullable=True),
        StructField("subtotal",       DoubleType(),  nullable=True),
        StructField("discount",       DoubleType(),  nullable=True),
        StructField("final_amount",   DoubleType(),  nullable=True),
        StructField("device",         StringType(),  nullable=True),
        StructField("status",         StringType(),  nullable=True),
        StructField("timestamp",      StringType(),  nullable=True),
        StructField("date",           StringType(),  nullable=True),
        StructField("hour",           IntegerType(), nullable=True),
        StructField("items", ArrayType(StructType([
            StructField("product_id",   StringType(), nullable=True),
            StructField("product_name", StringType(), nullable=True),
            StructField("category",     StringType(), nullable=True),
            StructField("unit_price",   DoubleType(), nullable=True),
            StructField("quantity",     IntegerType(),nullable=True),
            StructField("item_total",   DoubleType(), nullable=True),
        ])), nullable=True),
    ])


# ── SECTION 7: PAYMENT SCHEMA ─────────────────────────────────────
def get_payment_schema():
    return StructType([
        StructField("payment_id",     StringType(), nullable=True),
        StructField("order_id",       StringType(), nullable=True),
        StructField("customer_id",    StringType(), nullable=True),
        StructField("amount",         DoubleType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("status",         StringType(), nullable=True),
        StructField("gateway",        StringType(), nullable=True),
        StructField("transaction_id", StringType(), nullable=True),
        StructField("timestamp",      StringType(), nullable=True),
        StructField("date",           StringType(), nullable=True),
    ])


# ── SECTION 8: USER EVENT SCHEMA ──────────────────────────────────
def get_event_schema():
    return StructType([
        StructField("event_id",      StringType(), nullable=True),
        StructField("customer_id",   StringType(), nullable=True),
        StructField("event_type",    StringType(), nullable=True),
        StructField("product_id",    StringType(), nullable=True),
        StructField("product_name",  StringType(), nullable=True),
        StructField("category",      StringType(), nullable=True),
        StructField("search_query",  StringType(), nullable=True),
        StructField("device",        StringType(), nullable=True),
        StructField("session_id",    StringType(), nullable=True),
        StructField("page_url",      StringType(), nullable=True),
        StructField("timestamp",     StringType(), nullable=True),
        StructField("date",          StringType(), nullable=True),
    ])


# ── SECTION 9: READ FROM KAFKA ────────────────────────────────────
def read_from_kafka(spark, topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()


# ── SECTION 10: PARSE KAFKA STREAM ───────────────────────────────
def parse_kafka_stream(raw_df, schema):
    return raw_df \
        .select(
            col("value").cast("string").alias("json_str"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        ) \
        .select(
            from_json(col("json_str"), schema).alias("data"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
        ) \
        .select(
            "data.*",
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
        )


# ── SECTION 11: CLEAN ORDERS ─────────────────────────────────────
def clean_orders(raw_df):
    return raw_df \
        .filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("final_amount").isNotNull()) \
        .filter(col("final_amount") > 0) \
        .filter(col("num_items").isNotNull()) \
        .filter(col("num_items") > 0) \
        .withColumn("order_id",
            upper(trim(col("order_id")))) \
        .withColumn("customer_name",
            trim(col("customer_name"))) \
        .withColumn("city",
            trim(col("city"))) \
        .withColumn("region",
            upper(trim(col("region")))) \
        .withColumn("final_amount",
            spark_round(col("final_amount"), 2)) \
        .withColumn("discount",
            spark_round(
                when(col("discount").isNull(), lit(0.0))
                .otherwise(col("discount")), 2)) \
        .withColumn("event_timestamp",
            to_timestamp(col("timestamp"))) \
        .withColumn("is_high_value",
            when(col("final_amount") >= 50000, True)
            .otherwise(False)) \
        .withColumn("is_discounted",
            when(col("discount") > 0, True)
            .otherwise(False)) \
        .withColumn("order_day_of_week",
            dayofweek(col("event_timestamp"))) \
        .withColumn("order_hour",
            hour(col("event_timestamp"))) \
        .withColumn("value_segment",
            when(col("final_amount") >= 100000, lit("PREMIUM"))
            .when(col("final_amount") >= 50000,  lit("HIGH"))
            .when(col("final_amount") >= 10000,  lit("MEDIUM"))
            .otherwise(lit("LOW"))) \
        .withColumn("processed_at",
            current_timestamp()) \
        .withColumn("pipeline_version", lit("v2.0")) \
        .drop("address", "customer_phone")


# ── SECTION 12: CLEAN PAYMENTS ────────────────────────────────────
def clean_payments(raw_df):
    return raw_df \
        .filter(col("payment_id").isNotNull()) \
        .filter(col("order_id").isNotNull()) \
        .filter(col("amount").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("payment_id",
            upper(trim(col("payment_id")))) \
        .withColumn("order_id",
            upper(trim(col("order_id")))) \
        .withColumn("amount",
            spark_round(col("amount"), 2)) \
        .withColumn("payment_method",
            upper(trim(col("payment_method")))) \
        .withColumn("status",
            upper(trim(col("status")))) \
        .withColumn("is_successful",
            when(col("status") == "SUCCESS", True)
            .otherwise(False)) \
        .withColumn("event_timestamp",
            to_timestamp(col("timestamp"))) \
        .withColumn("payment_hour",
            hour(col("event_timestamp"))) \
        .withColumn("processed_at",
            current_timestamp()) \
        .withColumn("pipeline_version", lit("v2.0"))


# ── SECTION 13: CLEAN USER EVENTS ─────────────────────────────────
def clean_events(raw_df):
    return raw_df \
        .filter(col("event_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("event_type").isNotNull()) \
        .withColumn("event_id",
            upper(trim(col("event_id")))) \
        .withColumn("event_type",
            upper(trim(col("event_type")))) \
        .withColumn("device",
            upper(trim(col("device")))) \
        .withColumn("is_product_event",
            when(col("product_id").isNotNull(), True)
            .otherwise(False)) \
        .withColumn("is_search_event",
            when(col("event_type") == "SEARCH", True)
            .otherwise(False)) \
        .withColumn("event_timestamp",
            to_timestamp(col("timestamp"))) \
        .withColumn("event_hour",
            hour(col("event_timestamp"))) \
        .withColumn("processed_at",
            current_timestamp()) \
        .withColumn("pipeline_version", lit("v2.0"))


# ── SECTION 14: FOREACH BATCH WRITER ─────────────────────────────
def make_batch_writer(raw_path, curated_path, clean_fn, label):

    def process_batch(batch_df, batch_id):
        count = batch_df.count()

        if count == 0:
            print(f"⚡ [{label}] Batch {batch_id}: no records — skipping")
            return

        print(f"\n{'─'*50}")
        print(f"⚡ [{label}] Batch {batch_id}: {count} records received")

        # Write raw layer — exact copy, no changes
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(raw_path)
        print(f"   ✅ Raw layer written")

        # Apply cleaning function
        curated_df = clean_fn(batch_df)
        curated_count = curated_df.count()
        dropped = count - curated_count

        # Write curated layer — cleaned + enriched
        curated_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("date") \
            .save(curated_path)

        print(f"   ✅ Curated layer written")
        print(f"   📊 {count} in → {curated_count} clean ({dropped} dropped)")

    return process_batch


# ── SECTION 15: START STREAMING ───────────────────────────────────
def start_streaming(spark):

    schemas = {
        "orders":      get_order_schema(),
        "payments":    get_payment_schema(),
        "user_events": get_event_schema(),
    }
    clean_fns = {
        "orders":      clean_orders,
        "payments":    clean_payments,
        "user_events": clean_events,
    }
    raw_paths = {
        "orders":      RAW_ORDERS,
        "payments":    RAW_PAYMENTS,
        "user_events": RAW_EVENTS,
    }
    curated_paths = {
        "orders":      CUR_ORDERS,
        "payments":    CUR_PAYMENTS,
        "user_events": CUR_EVENTS,
    }
    chk_paths = {
        "orders":      CHK_ORDERS,
        "payments":    CHK_PAYMENTS,
        "user_events": CHK_EVENTS,
    }

    queries = []

    for topic in ["orders", "payments", "user_events"]:
        print(f"\n🚀 Starting pipeline for topic: {topic}")

        raw_kafka_df = read_from_kafka(spark, topic)
        parsed_df    = parse_kafka_stream(raw_kafka_df, schemas[topic])

        query = parsed_df \
            .writeStream \
            .foreachBatch(
                make_batch_writer(
                    raw_path     = raw_paths[topic],
                    curated_path = curated_paths[topic],
                    clean_fn     = clean_fns[topic],
                    label        = topic.upper()
                )
            ) \
            .option("checkpointLocation", chk_paths[topic]) \
            .trigger(processingTime="10 seconds") \
            .queryName(f"{topic}_pipeline") \
            .start()

        queries.append(query)
        print(f"   ✅ Pipeline started: {query.name}")

    return queries


# ── SECTION 16 + 17: MAIN ─────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("  🏪 E-COMMERCE ANALYTICS PLATFORM")
    print("  Phase 2: Spark Structured Streaming + Delta Lake")
    print("="*60 + "\n")

    create_directories()
    spark   = create_spark_session()
    queries = start_streaming(spark)

    print(f"\n{'='*60}")
    print(f"  ✅ ALL 3 PIPELINES RUNNING")
    print(f"  🌐 Spark UI:  http://localhost:4040")
    print(f"  📂 Data at:   {BASE_PATH}")
    print(f"  Press Ctrl+C to stop")
    print(f"{'='*60}\n")

    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all pipelines...")
        for query in queries:
            query.stop()
        spark.stop()
        print("✅ Stopped cleanly")
        print(f"📂 Delta Lake data saved at: {BASE_PATH}")


if __name__ == "__main__":
    main()
"""
╔══════════════════════════════════════════════════════════════════╗
║  E-COMMERCE ANALYTICS PLATFORM — AIRFLOW DAG                    ║
║  Phase 3: Nightly batch pipeline                                 ║
║                                                                  ║
║  Schedule: Every night at midnight                               ║
║  Tasks:                                                          ║
║    1. compute_daily_sales                                        ║
║    2. compute_rfm_scores                                         ║
║    3. identify_churn_candidates                                  ║
║    4. compute_payment_analysis                                   ║
║    5. pipeline_complete                                          ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ── IMPORTS ───────────────────────────────────────────────────────
from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ── PATHS ─────────────────────────────────────────────────────────
DELTA_BASE    = "/opt/delta-lake"
CURATED_BASE  = f"{DELTA_BASE}/curated"
SUMMARY_BASE  = f"{DELTA_BASE}/summaries"

ORDERS_PATH   = f"{CURATED_BASE}/orders"
PAYMENTS_PATH = f"{CURATED_BASE}/payments"
EVENTS_PATH   = f"{CURATED_BASE}/user_events"

DAILY_SALES_PATH    = f"{SUMMARY_BASE}/daily_sales"
CUSTOMER_RFM_PATH   = f"{SUMMARY_BASE}/customer_rfm"
CHURN_PATH          = f"{SUMMARY_BASE}/churn_candidates"
PAYMENT_STATS_PATH  = f"{SUMMARY_BASE}/payment_analysis"

# ── DEFAULT ARGUMENTS ─────────────────────────────────────────────
# These apply to EVERY task in the DAG unless overridden
default_args = {
    "owner":              "ecommerce-team",
    "depends_on_past":    False,
    "email_on_failure":   False,
    "email_on_retry":     False,
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),
    "execution_timeout":  timedelta(hours=2),
}

# ── DAG DEFINITION ────────────────────────────────────────────────
dag = DAG(
    dag_id            = "ecommerce_nightly_pipeline",
    description       = "Nightly batch: sales aggregation, RFM scores, churn detection",
    default_args      = default_args,
    schedule_interval = "0 0 * * *",
    start_date        = days_ago(1),
    catchup           = False,
    tags              = ["ecommerce", "batch", "nightly"],
    max_active_runs   = 1,
)

# ── TASK 1: COMPUTE DAILY SALES ───────────────────────────────────
def compute_daily_sales(**context):
    """
    Compute daily sales aggregations from the curated orders table.
    Writes results to Delta Lake summaries/daily_sales/

    context: Airflow passes execution context automatically
             context['ds'] = the execution date as string 'YYYY-MM-DD'
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, sum as spark_sum, count, avg,
        round as spark_round, when, max as spark_max,
        current_date, lit
    )

    # Get the execution date from Airflow context
    execution_date = context["ds"]
    print(f"\n{'='*50}")
    print(f"Computing daily sales for: {execution_date}")
    print(f"{'='*50}")

    # Create Spark session for this task
    spark = SparkSession.builder \
        .appName("DailySalesAggregation") \
        .master("local[2]") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Check if orders data exists
    if not os.path.exists(ORDERS_PATH):
        print("⚠️  No orders data found — skipping")
        spark.stop()
        return

    # Read curated orders
    orders_df = spark.read.format("delta").load(ORDERS_PATH)

    total_orders = orders_df.count()
    print(f"📦 Total orders available: {total_orders}")

    if total_orders == 0:
        print("⚠️  No orders data — skipping")
        spark.stop()
        return

    # ── Compute aggregations ──────────────────────────────────
    daily_summary = orders_df \
        .groupBy("date", "region") \
        .agg(
            count("order_id")
                .alias("total_orders"),
            spark_round(spark_sum("final_amount"), 2)
                .alias("total_revenue"),
            spark_round(avg("final_amount"), 2)
                .alias("avg_order_value"),
            spark_round(spark_sum("discount"), 2)
                .alias("total_discount"),
            spark_sum(when(col("is_high_value") == True, 1).otherwise(0))
                .alias("high_value_orders"),
            spark_sum(when(col("status") == "CANCELLED", 1).otherwise(0))
                .alias("cancelled_orders"),
            spark_sum(when(col("device") == "MOBILE", 1).otherwise(0))
                .alias("mobile_orders"),
            spark_sum(when(col("device") == "DESKTOP", 1).otherwise(0))
                .alias("desktop_orders"),
        ) \
        .withColumn("computed_at", lit(execution_date))

    # Write to Delta Lake summaries
    os.makedirs(DAILY_SALES_PATH, exist_ok=True)
    daily_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(DAILY_SALES_PATH)

    result_count = daily_summary.count()
    print(f"✅ Daily sales summary written: {result_count} rows")
    print(f"   Saved to: {DAILY_SALES_PATH}")

    daily_summary.show(10)
    spark.stop()

    # ── TASK 2: COMPUTE RFM SCORES ────────────────────────────────────
def compute_rfm_scores(**context):
    """
    RFM Analysis — Recency, Frequency, Monetary.

    Recency:   How recently did the customer order? (lower days = better)
    Frequency: How many times have they ordered? (higher = better)
    Monetary:  How much have they spent in total? (higher = better)

    Each scored 1-5. RFM segment = combination of all three.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, sum as spark_sum, avg,
        round as spark_round, max as spark_max,
        datediff, current_date, lit, when,
        ntile, desc
    )
    from pyspark.sql.window import Window

    execution_date = context["ds"]
    print(f"\n{'='*50}")
    print(f"Computing RFM scores as of: {execution_date}")
    print(f"{'='*50}")

    spark = SparkSession.builder \
        .appName("RFMScoreComputation") \
        .master("local[2]") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    if not os.path.exists(ORDERS_PATH):
        print("⚠️  No orders data — skipping RFM")
        spark.stop()
        return

    orders_df = spark.read.format("delta").load(ORDERS_PATH)

    if orders_df.count() == 0:
        print("⚠️  Empty orders table — skipping RFM")
        spark.stop()
        return

    # ── Step 1: Compute raw RFM metrics per customer ──────────
    customer_metrics = orders_df \
        .groupBy("customer_id", "customer_name", "customer_email") \
        .agg(
            spark_max("date").alias("last_order_date"),
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("final_amount"), 2).alias("total_revenue"),
            spark_round(avg("final_amount"), 2).alias("avg_order_value"),
        )

    # ── Step 2: Compute days since last order ─────────────────
    customer_metrics = customer_metrics \
        .withColumn("days_since_order",
            datediff(current_date(), col("last_order_date")))

    # ── Step 3: Score each dimension 1-5 using ntile ──────────
    # ntile(5) divides customers into 5 equal groups
    # Group 1 = bottom 20%, Group 5 = top 20%

    window_recency   = Window.orderBy(desc("days_since_order"))
    window_frequency = Window.orderBy("total_orders")
    window_monetary  = Window.orderBy("total_revenue")

    customer_metrics = customer_metrics \
        .withColumn("recency_score",
            # Lower days_since_order = more recent = better = higher score
            # So we reverse: 5 for most recent, 1 for least recent
            (6 - ntile(5).over(window_recency))) \
        .withColumn("frequency_score",
            ntile(5).over(window_frequency)) \
        .withColumn("monetary_score",
            ntile(5).over(window_monetary))

    # ── Step 4: Assign RFM segment ────────────────────────────
    rfm_df = customer_metrics \
        .withColumn("rfm_total",
            col("recency_score") +
            col("frequency_score") +
            col("monetary_score")) \
        .withColumn("rfm_segment",
            when(col("rfm_total") >= 13, lit("CHAMPIONS"))
            .when(col("rfm_total") >= 10, lit("LOYAL_CUSTOMERS"))
            .when(col("rfm_total") >= 7,  lit("POTENTIAL_LOYALISTS"))
            .when(col("rfm_total") >= 5,  lit("AT_RISK"))
            .otherwise(lit("LOST_CUSTOMERS"))) \
        .withColumn("computed_date", lit(execution_date))

    # ── Step 5: Write results ─────────────────────────────────
    os.makedirs(CUSTOMER_RFM_PATH, exist_ok=True)
    rfm_df.select(
        "customer_id", "customer_name", "customer_email",
        "last_order_date", "days_since_order",
        "total_orders", "total_revenue", "avg_order_value",
        "recency_score", "frequency_score", "monetary_score",
        "rfm_segment", "computed_date"
    ).write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(CUSTOMER_RFM_PATH)

    result_count = rfm_df.count()
    print(f"✅ RFM scores computed: {result_count} customers")

    # Show segment distribution
    print("\n📊 RFM Segment Distribution:")
    rfm_df.groupBy("rfm_segment").count() \
          .orderBy("count", ascending=False).show()

    spark.stop()

    # ── TASK 3: IDENTIFY CHURN CANDIDATES ─────────────────────────────
def identify_churn_candidates(**context):
    """
    Find customers who haven't ordered in 30, 60, or 90+ days.
    These are churn risk customers who need re-engagement campaigns.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, sum as spark_sum, round as spark_round,
        max as spark_max, datediff, current_date,
        lit, when
    )

    execution_date = context["ds"]
    print(f"\n{'='*50}")
    print(f"Identifying churn candidates as of: {execution_date}")
    print(f"{'='*50}")

    spark = SparkSession.builder \
        .appName("ChurnDetection") \
        .master("local[2]") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    if not os.path.exists(ORDERS_PATH):
        print("⚠️  No orders data — skipping churn")
        spark.stop()
        return

    orders_df = spark.read.format("delta").load(ORDERS_PATH)

    if orders_df.count() == 0:
        print("⚠️  Empty orders table — skipping churn")
        spark.stop()
        return

    # ── Customer last order metrics ───────────────────────────
    customer_last_order = orders_df \
        .groupBy("customer_id", "customer_name", "customer_email") \
        .agg(
            spark_max("date").alias("last_order_date"),
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("final_amount"), 2)
                .alias("total_lifetime_value"),
        ) \
        .withColumn("days_since_order",
            datediff(current_date(), col("last_order_date")))

    # ── Classify churn risk ───────────────────────────────────
    churn_df = customer_last_order \
        .withColumn("churn_risk",
            when(col("days_since_order") >= 90, lit("HIGH"))
            .when(col("days_since_order") >= 60, lit("MEDIUM"))
            .when(col("days_since_order") >= 30, lit("LOW"))
            .otherwise(lit("ACTIVE"))) \
        .filter(col("churn_risk") != "ACTIVE") \
        .withColumn("computed_date", lit(execution_date))

    # ── Write results ─────────────────────────────────────────
    os.makedirs(CHURN_PATH, exist_ok=True)
    churn_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(CHURN_PATH)

    total_churn = churn_df.count()
    print(f"✅ Churn candidates identified: {total_churn}")

    print("\n📊 Churn Risk Breakdown:")
    churn_df.groupBy("churn_risk").count() \
            .orderBy("count", ascending=False).show()

    spark.stop()

    # ── TASK 4: COMPUTE PAYMENT ANALYSIS ──────────────────────────────
def compute_payment_analysis(**context):
    """
    Analyse payment method performance — success rates, volumes.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, sum as spark_sum,
        round as spark_round, when, lit
    )

    execution_date = context["ds"]
    print(f"\n{'='*50}")
    print(f"Computing payment analysis for: {execution_date}")
    print(f"{'='*50}")

    spark = SparkSession.builder \
        .appName("PaymentAnalysis") \
        .master("local[2]") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    if not os.path.exists(PAYMENTS_PATH):
        print("⚠️  No payments data — skipping")
        spark.stop()
        return

    payments_df = spark.read.format("delta").load(PAYMENTS_PATH)

    if payments_df.count() == 0:
        print("⚠️  Empty payments table — skipping")
        spark.stop()
        return

    payment_stats = payments_df \
        .groupBy("date", "payment_method") \
        .agg(
            count("payment_id").alias("total_transactions"),
            spark_sum(when(col("is_successful") == True, 1)
                .otherwise(0)).alias("successful"),
            spark_sum(when(col("is_successful") == False, 1)
                .otherwise(0)).alias("failed"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
        ) \
        .withColumn("success_rate",
            spark_round(
                col("successful") * 100.0 / col("total_transactions"),
            2)) \
        .withColumn("computed_date", lit(execution_date))

    os.makedirs(PAYMENT_STATS_PATH, exist_ok=True)
    payment_stats.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(PAYMENT_STATS_PATH)

    print(f"✅ Payment analysis written: {payment_stats.count()} rows")
    payment_stats.show(10)
    spark.stop()

    # ── DEFINE TASKS ──────────────────────────────────────────────────
with dag:

    # Start marker — visual node in the DAG graph
    pipeline_start = EmptyOperator(
        task_id = "pipeline_start"
    )

    # Task 1 — Daily sales aggregation
    task_daily_sales = PythonOperator(
        task_id         = "compute_daily_sales",
        python_callable = compute_daily_sales,
        provide_context = True,
    )

    # Task 2 — RFM scores
    task_rfm = PythonOperator(
        task_id         = "compute_rfm_scores",
        python_callable = compute_rfm_scores,
        provide_context = True,
    )

    # Task 3 — Churn detection
    task_churn = PythonOperator(
        task_id         = "identify_churn_candidates",
        python_callable = identify_churn_candidates,
        provide_context = True,
    )

    # Task 4 — Payment analysis
    task_payments = PythonOperator(
        task_id         = "compute_payment_analysis",
        python_callable = compute_payment_analysis,
        provide_context = True,
    )

    # End marker
    pipeline_complete = EmptyOperator(
        task_id = "pipeline_complete"
    )

    # ── TASK DEPENDENCIES ─────────────────────────────────────
    # Define the ORDER tasks run in
    #
    #                pipeline_start
    #                      │
    #              task_daily_sales
    #                      │
    #          ┌───────────┼───────────┐
    #          ▼           ▼           ▼
    #       task_rfm  task_churn  task_payments
    #          │           │           │
    #          └───────────┼───────────┘
    #                      ▼
    #              pipeline_complete

    pipeline_start >> task_daily_sales
    task_daily_sales >> [task_rfm, task_churn, task_payments]
    [task_rfm, task_churn, task_payments] >> pipeline_complete

    
"""
Verify Delta Lake data — run this AFTER streaming_job.py
has been running for at least 30 seconds.
"""
import os

# ── WINDOWS FIX ───────────────────────────────────────────────────
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

# ── PATHS ─────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
BASE_PATH    = os.path.join(PROJECT_ROOT, "delta-lake")

print(f"\n📂 Looking for Delta Lake data at:")
print(f"   {BASE_PATH}\n")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaVerifier") \
    .master("local[2]") \
    .config("spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("  📊 DELTA LAKE VERIFICATION")
print("=" * 60)

# ── Helper ────────────────────────────────────────────────────────
def check_table(label, path, show_cols, group_col=None):
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"  Path: {path}")

    # Check if folder exists at all
    if not os.path.exists(path):
        print(f"  ⏳ Folder doesn't exist yet.")
        print(f"     → Make sure streaming_job.py is running first!")
        print(f"     → Wait at least 30 seconds after starting it.")
        return

    # Check if _delta_log exists (confirms it's a Delta table)
    delta_log = os.path.join(path, "_delta_log")
    if not os.path.exists(delta_log):
        print(f"  ⏳ Folder exists but no Delta table written yet.")
        print(f"     → Wait a few more seconds for the first batch.")
        return

    # Read and display
    try:
        df = spark.read.format("delta").load(path)
        total = df.count()
        print(f"  ✅ Total records: {total}")

        if total == 0:
            print(f"  ⏳ Table exists but no records yet — wait for next batch")
            return

        print(f"\n  Sample records:")
        df.select(show_cols).show(5, truncate=False)

        if group_col:
            print(f"  Breakdown by {group_col}:")
            df.groupBy(group_col).count() \
              .orderBy("count", ascending=False) \
              .show(10)

    except Exception as e:
        print(f"  ❌ Error reading table: {e}")

# ── Check each table ──────────────────────────────────────────────
check_table(
    label     = "📦 ORDERS — Curated Layer",
    path      = os.path.join(BASE_PATH, "curated", "orders"),
    show_cols = ["order_id", "customer_name", "city",
                 "region", "final_amount", "value_segment",
                 "is_high_value", "order_hour"],
    group_col = "value_segment"
)

check_table(
    label     = "💳 PAYMENTS — Curated Layer",
    path      = os.path.join(BASE_PATH, "curated", "payments"),
    show_cols = ["payment_id", "order_id", "amount",
                 "payment_method", "status", "is_successful"],
    group_col = "payment_method"
)

check_table(
    label     = "🖱️  USER EVENTS — Curated Layer",
    path      = os.path.join(BASE_PATH, "curated", "user_events"),
    show_cols = ["event_id", "customer_id", "event_type",
                 "device", "is_search_event"],
    group_col = "event_type"
)

# ── Show folder structure ─────────────────────────────────────────
print(f"\n{'─'*50}")
print(f"📂 Delta Lake folder structure:")
for root, dirs, files in os.walk(BASE_PATH):
    dirs[:] = [d for d in dirs if d != "_delta_log"]
    level = root.replace(BASE_PATH, "").count(os.sep)
    indent = "   " * level
    folder = os.path.basename(root)
    if level <= 3:
        parquet_count = len([f for f in files if f.endswith(".parquet")])
        if parquet_count > 0:
            print(f"{indent}📁 {folder}/ ({parquet_count} parquet files)")
        else:
            print(f"{indent}📁 {folder}/")

spark.stop()
print(f"\n✅ Verification complete")
print(f"📂 Delta Lake is at: {BASE_PATH}")
"""
╔══════════════════════════════════════════════════════════════════╗
║  E-COMMERCE ANALYTICS PLATFORM — ANALYTICS JOB                  ║
║  Phase 4: PySpark analytics + chart generation + Excel export   ║
║                                                                  ║
║  Analyses:                                                       ║
║    1. Revenue by Region                                          ║
║    2. Sales Trend Over Time                                      ║
║    3. Peak Hours Analysis                                        ║
║    4. Value Segment Distribution                                 ║
║    5. Payment Method Analysis                                    ║
║    6. Customer RFM Distribution                                  ║
║    7. Device Breakdown                                           ║
║    8. Top 10 Cities by Revenue                                   ║
║    9. User Event Funnel                                          ║
║                                                                  ║
║  Output:                                                         ║
║    → 8 PNG charts in analytics/charts/                          ║
║    → 1 Excel file with 10 sheets                                ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ── STEP 1: ENVIRONMENT SETUP (before any other imports) ──────────
import os
import sys

# Move Spark temp files to D: drive (away from full C: drive)
os.environ["HADOOP_HOME"]       = r"C:\hadoop"
os.environ["PATH"]              = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["USERPROFILE"]       = r"D:\spark-cache"
os.environ["HOME"]              = r"D:\spark-cache"
os.environ["IVY_HOME"]          = r"D:\spark-cache\.ivy2"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.io.tmpdir=D:\\spark-tmp"

# ── STEP 2: PYTHON IMPORTS ────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg,
    round as spark_round, when, lit,
    max as spark_max, min as spark_min,
    desc, asc
)
from pyspark.sql.window import Window

import matplotlib
matplotlib.use("Agg")   # save to file — no display window needed
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ── STEP 3: PATHS ─────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
BASE_PATH    = os.path.join(PROJECT_ROOT, "delta-lake")

CURATED_PATH = os.path.join(BASE_PATH, "curated")
SUMMARY_PATH = os.path.join(BASE_PATH, "summaries")
CHARTS_PATH  = os.path.join(SCRIPT_DIR, "charts")
EXCEL_PATH   = os.path.join(SCRIPT_DIR,
               "Ecommerce_Analytics_Report.xlsx")

# Create charts folder
os.makedirs(CHARTS_PATH, exist_ok=True)

# ── STEP 4: CHART STYLE ───────────────────────────────────────────
plt.style.use("seaborn-v0_8-whitegrid")

COLORS = {
    "primary":   "#1F4E79",
    "secondary": "#2E75B6",
    "accent":    "#E67E22",
    "green":     "#1F6E43",
    "light":     "#D6E4F7",
    "red":       "#C0392B",
    "purple":    "#6C3483",
    "gray":      "#7F8C8D",
}

REGION_COLORS = {
    "NORTH":   "#1F4E79",
    "SOUTH":   "#2E75B6",
    "EAST":    "#E67E22",
    "WEST":    "#1F6E43",
    "CENTRAL": "#6C3483",
}

SEG_COLORS = {
    "PREMIUM": "#6C3483",
    "HIGH":    "#1F4E79",
    "MEDIUM":  "#2E75B6",
    "LOW":     "#AED6F1",
}

RFM_COLORS = {
    "CHAMPIONS":           "#1F4E79",
    "LOYAL_CUSTOMERS":     "#2E75B6",
    "POTENTIAL_LOYALISTS": "#5499C7",
    "AT_RISK":             "#E67E22",
    "LOST_CUSTOMERS":      "#C0392B",
}


# ══════════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════
def create_spark():
    spark = SparkSession.builder \
        .appName("EcommerceAnalytics") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.local.dir", "D:\\spark-tmp") \
        .config("spark.ui.port", "4041") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ══════════════════════════════════════════════════════════════════
# LOAD DELTA LAKE TABLES
# ══════════════════════════════════════════════════════════════════
def load_tables(spark):
    """Load Delta Lake tables as Spark SQL views."""
    print("\n📂 Loading Delta Lake tables...")

    def safe_load(path, view_name, label):
        """Load a Delta table safely — skip if missing."""
        if not os.path.exists(path):
            print(f"   ⚠️  {label} not found — skipping")
            return 0
        delta_log = os.path.join(path, "_delta_log")
        if not os.path.exists(delta_log):
            print(f"   ⚠️  {label} has no data yet — skipping")
            return 0
        try:
            spark.read.format("delta").load(path) \
                 .createOrReplaceTempView(view_name)
            n = spark.sql(
                f"SELECT COUNT(*) AS n FROM {view_name}"
            ).collect()[0]["n"]
            print(f"   ✅ {label}: {n:,} records")
            return n
        except Exception as e:
            print(f"   ❌ {label} failed: {e}")
            return 0

    safe_load(os.path.join(CURATED_PATH, "orders"),
              "orders",      "orders")
    safe_load(os.path.join(CURATED_PATH, "payments"),
              "payments",    "payments")
    safe_load(os.path.join(CURATED_PATH, "user_events"),
              "user_events", "user_events")
    safe_load(os.path.join(SUMMARY_PATH, "customer_rfm"),
              "customer_rfm","customer_rfm")
    safe_load(os.path.join(SUMMARY_PATH, "churn_candidates"),
              "churn_candidates", "churn_candidates")


# ══════════════════════════════════════════════════════════════════
# HELPER — SAVE CHART
# ══════════════════════════════════════════════════════════════════
def save_chart(fig, filename):
    """Save chart to charts/ folder and close figure."""
    path = os.path.join(CHARTS_PATH, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight",
                facecolor="white")
    plt.close(fig)
    print(f"   💾 Saved: {filename}")
    return path


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 1 — REVENUE BY REGION
# ══════════════════════════════════════════════════════════════════
def analyse_revenue_by_region(spark):
    print("\n📊 Analysis 1: Revenue by Region")
    try:
        df = spark.sql("""
            SELECT
                region,
                COUNT(order_id)                 AS total_orders,
                ROUND(SUM(final_amount), 2)     AS total_revenue,
                ROUND(AVG(final_amount), 2)     AS avg_order_value,
                SUM(CASE WHEN is_high_value = true
                    THEN 1 ELSE 0 END)          AS high_value_orders,
                ROUND(SUM(discount), 2)         AS total_discount
            FROM orders
            GROUP BY region
            ORDER BY total_revenue DESC
        """)
        df.show()
        pdf = df.toPandas()

        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle("Revenue Analysis by Region",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        # Left — revenue bar
        bar_colors = [REGION_COLORS.get(r, COLORS["secondary"])
                      for r in pdf["region"]]
        bars = axes[0].barh(pdf["region"], pdf["total_revenue"],
                            color=bar_colors, edgecolor="white")
        axes[0].set_title("Total Revenue",
                          fontsize=11, color=COLORS["primary"])
        axes[0].xaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda x, _: f"₹{x/100000:.1f}L"))
        for bar, val in zip(bars, pdf["total_revenue"]):
            axes[0].text(
                bar.get_width() * 1.01,
                bar.get_y() + bar.get_height() / 2,
                f"₹{val/100000:.1f}L",
                va="center", fontsize=9)

        # Right — orders vs high value
        x = range(len(pdf))
        w = 0.35
        axes[1].bar([i - w/2 for i in x], pdf["total_orders"],
                    w, label="Total Orders",
                    color=COLORS["secondary"], alpha=0.8)
        axes[1].bar([i + w/2 for i in x],
                    pdf["high_value_orders"],
                    w, label="High Value",
                    color=COLORS["accent"], alpha=0.8)
        axes[1].set_xticks(list(x))
        axes[1].set_xticklabels(pdf["region"], fontsize=9)
        axes[1].set_title("Orders vs High Value Orders",
                          fontsize=11, color=COLORS["primary"])
        axes[1].legend(fontsize=9)

        plt.tight_layout()
        save_chart(fig, "01_revenue_by_region.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 2 — SALES TREND OVER TIME
# ══════════════════════════════════════════════════════════════════
def analyse_sales_trend(spark):
    print("\n📊 Analysis 2: Sales Trend Over Time")
    try:
        df = spark.sql("""
            SELECT
                date,
                COUNT(order_id)             AS total_orders,
                ROUND(SUM(final_amount), 2) AS daily_revenue,
                ROUND(AVG(final_amount), 2) AS avg_order_value,
                SUM(CASE WHEN discount > 0
                    THEN 1 ELSE 0 END)      AS discounted_orders
            FROM orders
            GROUP BY date
            ORDER BY date ASC
        """)
        df.show()
        pdf = df.toPandas()
        pdf["date"] = pd.to_datetime(pdf["date"])

        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle("Sales Trend Over Time",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        # Revenue line
        axes[0].fill_between(pdf["date"], pdf["daily_revenue"],
                             alpha=0.3,
                             color=COLORS["secondary"])
        axes[0].plot(pdf["date"], pdf["daily_revenue"],
                     color=COLORS["primary"], linewidth=2,
                     marker="o", markersize=4)
        axes[0].set_title("Daily Revenue",
                          fontsize=11, color=COLORS["primary"])
        axes[0].set_ylabel("Revenue (₹)")
        axes[0].yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda x, _: f"₹{x/100000:.1f}L"))
        axes[0].tick_params(axis="x", rotation=45)

        # Orders bar
        axes[1].bar(pdf["date"], pdf["total_orders"],
                    color=COLORS["secondary"],
                    alpha=0.8, width=0.8)
        axes[1].plot(pdf["date"], pdf["discounted_orders"],
                     color=COLORS["accent"], linewidth=2,
                     marker="s", markersize=4,
                     label="Discounted Orders")
        axes[1].set_title("Daily Orders",
                          fontsize=11, color=COLORS["primary"])
        axes[1].set_ylabel("Order Count")
        axes[1].legend(fontsize=9)
        axes[1].tick_params(axis="x", rotation=45)

        plt.tight_layout()
        save_chart(fig, "02_sales_trend.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 3 — PEAK HOURS
# ══════════════════════════════════════════════════════════════════
def analyse_peak_hours(spark):
    print("\n📊 Analysis 3: Peak Hours Analysis")
    try:
        df = spark.sql("""
            SELECT
                order_hour,
                order_day_of_week,
                COUNT(order_id)             AS total_orders,
                ROUND(SUM(final_amount), 2) AS total_revenue
            FROM orders
            GROUP BY order_hour, order_day_of_week
            ORDER BY order_day_of_week, order_hour
        """)
        df.show(10)
        pdf = df.toPandas()

        pivot = pdf.pivot_table(
            index="order_hour",
            columns="order_day_of_week",
            values="total_orders",
            aggfunc="sum",
            fill_value=0
        )
        day_names = {1:"Sun", 2:"Mon", 3:"Tue",
                     4:"Wed", 5:"Thu", 6:"Fri", 7:"Sat"}
        pivot.columns = [day_names.get(c, c)
                         for c in pivot.columns]

        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle("Peak Hours Analysis",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        # Heatmap
        sns.heatmap(pivot, ax=axes[0], cmap="YlOrRd",
                    annot=True, fmt="d", linewidths=0.5,
                    cbar_kws={"label": "Orders"})
        axes[0].set_title("Orders by Hour and Day",
                          fontsize=11, color=COLORS["primary"])
        axes[0].set_xlabel("Day of Week")
        axes[0].set_ylabel("Hour of Day (0-23)")

        # Hourly bar
        hourly = pdf.groupby("order_hour")["total_orders"] \
                    .sum().reset_index()
        bar_colors = [
            COLORS["accent"] if h in [19, 20, 21, 22]
            else COLORS["secondary"]
            for h in hourly["order_hour"]
        ]
        axes[1].bar(hourly["order_hour"],
                    hourly["total_orders"],
                    color=bar_colors, edgecolor="white")
        axes[1].set_title("Orders by Hour of Day",
                          fontsize=11, color=COLORS["primary"])
        axes[1].set_xlabel("Hour (0-23)")
        axes[1].set_ylabel("Total Orders")
        axes[1].set_xticks(range(0, 24, 2))

        plt.tight_layout()
        save_chart(fig, "03_peak_hours.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 4 — VALUE SEGMENTS
# ══════════════════════════════════════════════════════════════════
def analyse_value_segments(spark):
    print("\n📊 Analysis 4: Value Segment Distribution")
    try:
        df = spark.sql("""
            SELECT
                value_segment,
                COUNT(order_id)             AS total_orders,
                ROUND(SUM(final_amount), 2) AS total_revenue,
                ROUND(AVG(final_amount), 2) AS avg_order_value,
                ROUND(AVG(num_items), 2)    AS avg_items
            FROM orders
            GROUP BY value_segment
            ORDER BY
                CASE value_segment
                    WHEN 'PREMIUM' THEN 1
                    WHEN 'HIGH'    THEN 2
                    WHEN 'MEDIUM'  THEN 3
                    WHEN 'LOW'     THEN 4
                END
        """)
        df.show()
        pdf = df.toPandas()
        chart_colors = [SEG_COLORS.get(s, COLORS["secondary"])
                        for s in pdf["value_segment"]]

        fig, axes = plt.subplots(1, 2, figsize=(14, 7))
        fig.suptitle("Customer Value Segment Analysis",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        # Donut
        axes[0].pie(
            pdf["total_orders"],
            labels=pdf["value_segment"],
            colors=chart_colors,
            autopct="%1.1f%%",
            startangle=90,
            pctdistance=0.8,
            wedgeprops=dict(width=0.6,
                            edgecolor="white",
                            linewidth=2)
        )
        axes[0].set_title("Order Share by Segment",
                          fontsize=11, color=COLORS["primary"])

        # Revenue bar
        bars = axes[1].bar(
            range(len(pdf)), pdf["total_revenue"],
            color=chart_colors, edgecolor="white"
        )
        axes[1].set_xticks(range(len(pdf)))
        axes[1].set_xticklabels(pdf["value_segment"],
                                fontsize=9)
        axes[1].set_title("Revenue by Segment",
                          fontsize=11, color=COLORS["primary"])
        axes[1].set_ylabel("Revenue (₹)")
        axes[1].yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"₹{v/100000:.1f}L"))
        for bar, val in zip(bars, pdf["total_revenue"]):
            axes[1].text(
                bar.get_x() + bar.get_width()/2,
                bar.get_height() * 1.01,
                f"₹{val/100000:.1f}L",
                ha="center", fontsize=9)

        plt.tight_layout()
        save_chart(fig, "04_value_segments.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 5 — PAYMENT METHODS
# ══════════════════════════════════════════════════════════════════
def analyse_payments(spark):
    print("\n📊 Analysis 5: Payment Method Analysis")
    try:
        df = spark.sql("""
            SELECT
                payment_method,
                COUNT(payment_id)                       AS total_transactions,
                SUM(CASE WHEN is_successful = true
                    THEN 1 ELSE 0 END)                  AS successful,
                SUM(CASE WHEN is_successful = false
                    THEN 1 ELSE 0 END)                  AS failed,
                ROUND(
                    SUM(CASE WHEN is_successful = true
                        THEN 1 ELSE 0 END) * 100.0
                    / COUNT(payment_id), 2)             AS success_rate,
                ROUND(AVG(amount), 2)                   AS avg_transaction
            FROM payments
            GROUP BY payment_method
            ORDER BY total_transactions DESC
        """)
        df.show()
        pdf = df.toPandas()

        for col_name in ["total_transactions", "successful",
                 "failed", "success_rate", "avg_transaction"]:
         if col_name in pdf.columns:
           pdf[col_name] = pdf[col_name].astype(float)

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle("Payment Method Analysis",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        pal = sns.color_palette("Blues_d", len(pdf))

        # Volume
        bars = axes[0][0].bar(range(len(pdf)),
                               pdf["total_transactions"],
                               color=pal, edgecolor="white")
        axes[0][0].set_xticks(range(len(pdf)))
        axes[0][0].set_xticklabels(pdf["payment_method"],
                                    rotation=20, ha="right",
                                    fontsize=8)
        axes[0][0].set_title("Transaction Volume",
                              fontsize=11,
                              color=COLORS["primary"])

        # Success rate
        sr_colors = [
            COLORS["green"] if r >= 90 else
            COLORS["accent"] if r >= 80 else
            COLORS["red"]
            for r in pdf["success_rate"]
        ]
        bars2 = axes[0][1].barh(pdf["payment_method"],
                                  pdf["success_rate"],
                                  color=sr_colors,
                                  edgecolor="white")
        axes[0][1].set_xlim(0, 100)
        axes[0][1].axvline(x=90, color="gray",
                            linestyle="--", alpha=0.7)
        axes[0][1].set_title("Success Rate (%)",
                              fontsize=11,
                              color=COLORS["primary"])
        for bar, val in zip(bars2, pdf["success_rate"]):
            axes[0][1].text(
                val + 0.5,
                bar.get_y() + bar.get_height()/2,
                f"{val}%", va="center", fontsize=8)

        # Stacked success vs failed
        axes[1][0].bar(range(len(pdf)), pdf["successful"],
                        label="Successful",
                        color=COLORS["green"], alpha=0.8)
        axes[1][0].bar(range(len(pdf)), pdf["failed"],
                        bottom=pdf["successful"],
                        label="Failed",
                        color=COLORS["red"], alpha=0.8)
        axes[1][0].set_xticks(range(len(pdf)))
        axes[1][0].set_xticklabels(pdf["payment_method"],
                                    rotation=20, ha="right",
                                    fontsize=8)
        axes[1][0].set_title("Success vs Failed",
                              fontsize=11,
                              color=COLORS["primary"])
        axes[1][0].legend(fontsize=9)

        # Avg transaction value
        axes[1][1].bar(range(len(pdf)), pdf["avg_transaction"],
                        color=COLORS["secondary"],
                        edgecolor="white")
        axes[1][1].set_xticks(range(len(pdf)))
        axes[1][1].set_xticklabels(pdf["payment_method"],
                                    rotation=20, ha="right",
                                    fontsize=8)
        axes[1][1].set_title("Avg Transaction Value (₹)",
                              fontsize=11,
                              color=COLORS["primary"])
        axes[1][1].yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"₹{v:,.0f}"))

        plt.tight_layout()
        save_chart(fig, "05_payment_methods.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 6 — CUSTOMER RFM
# ══════════════════════════════════════════════════════════════════
def analyse_rfm(spark):
    print("\n📊 Analysis 6: Customer RFM Distribution")
    try:
        n = spark.sql(
            "SELECT COUNT(*) AS n FROM customer_rfm"
        ).collect()[0]["n"]
        if n == 0:
            print("   ⚠️  customer_rfm is empty — run Airflow DAG first")
            return None
    except Exception:
        print("   ⚠️  customer_rfm not available — run Airflow DAG first")
        return None

    df = spark.sql("""
        SELECT
            rfm_segment,
            COUNT(customer_id)              AS customer_count,
            ROUND(AVG(total_revenue), 2)    AS avg_lifetime_value,
            ROUND(AVG(total_orders), 2)     AS avg_orders,
            ROUND(AVG(days_since_order), 2) AS avg_days_inactive
        FROM customer_rfm
        GROUP BY rfm_segment
        ORDER BY avg_lifetime_value DESC
    """)
    df.show()
    pdf = df.toPandas()

    seg_order = ["CHAMPIONS", "LOYAL_CUSTOMERS",
                 "POTENTIAL_LOYALISTS", "AT_RISK",
                 "LOST_CUSTOMERS"]
    pdf["rfm_segment"] = pd.Categorical(
        pdf["rfm_segment"],
        categories=seg_order, ordered=True
    )
    pdf = pdf.sort_values("rfm_segment").dropna(
        subset=["rfm_segment"])
    chart_colors = [RFM_COLORS.get(s, COLORS["secondary"])
                    for s in pdf["rfm_segment"]]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("Customer RFM Segmentation",
                 fontsize=15, fontweight="bold",
                 color=COLORS["primary"])

    # Donut
    axes[0].pie(
        pdf["customer_count"],
        labels=pdf["rfm_segment"],
        colors=chart_colors,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.75,
        wedgeprops=dict(width=0.55,
                        edgecolor="white",
                        linewidth=2)
    )
    axes[0].set_title("Customer Distribution",
                      fontsize=11, color=COLORS["primary"])

    # Avg lifetime value
    bars = axes[1].bar(range(len(pdf)),
                        pdf["avg_lifetime_value"],
                        color=chart_colors,
                        edgecolor="white")
    axes[1].set_xticks(range(len(pdf)))
    axes[1].set_xticklabels(
        [s.replace("_", "\n") for s in pdf["rfm_segment"]],
        fontsize=8
    )
    axes[1].set_title("Avg Lifetime Value by Segment",
                      fontsize=11, color=COLORS["primary"])
    axes[1].set_ylabel("Avg Lifetime Value (₹)")
    axes[1].yaxis.set_major_formatter(
        mticker.FuncFormatter(
            lambda v, _: f"₹{v:,.0f}"))
    for bar, val in zip(bars, pdf["avg_lifetime_value"]):
        axes[1].text(
            bar.get_x() + bar.get_width()/2,
            bar.get_height() * 1.01,
            f"₹{val:,.0f}",
            ha="center", fontsize=8,
            color=COLORS["primary"])

    plt.tight_layout()
    save_chart(fig, "06_rfm_distribution.png")
    return pdf


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 7 — DEVICE BREAKDOWN
# ══════════════════════════════════════════════════════════════════
def analyse_devices(spark):
    print("\n📊 Analysis 7: Device Breakdown")
    try:
        df = spark.sql("""
            SELECT
                device,
                COUNT(order_id)             AS total_orders,
                ROUND(SUM(final_amount), 2) AS total_revenue,
                ROUND(AVG(final_amount), 2) AS avg_order_value
            FROM orders
            GROUP BY device
            ORDER BY total_orders DESC
        """)
        df.show()
        pdf = df.toPandas()

        dev_colors = {
            "MOBILE":  COLORS["primary"],
            "DESKTOP": COLORS["secondary"],
            "TABLET":  COLORS["accent"],
        }
        chart_colors = [dev_colors.get(d, COLORS["secondary"])
                        for d in pdf["device"]]

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle("Device Breakdown Analysis",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        # Orders donut
        axes[0].pie(
            pdf["total_orders"],
            labels=pdf["device"],
            colors=chart_colors,
            autopct="%1.1f%%",
            startangle=90,
            wedgeprops=dict(width=0.6,
                            edgecolor="white",
                            linewidth=3)
        )
        axes[0].set_title("Order Share",
                          fontsize=11, color=COLORS["primary"])

        # Revenue bar
        axes[1].bar(pdf["device"], pdf["total_revenue"],
                    color=chart_colors, edgecolor="white")
        axes[1].set_title("Total Revenue",
                          fontsize=11, color=COLORS["primary"])
        axes[1].set_ylabel("Revenue (₹)")
        axes[1].yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"₹{v/100000:.1f}L"))

        # Avg order value
        bars = axes[2].bar(pdf["device"],
                            pdf["avg_order_value"],
                            color=chart_colors,
                            edgecolor="white")
        axes[2].set_title("Avg Order Value (₹)",
                          fontsize=11, color=COLORS["primary"])
        axes[2].yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"₹{v:,.0f}"))
        for bar, val in zip(bars, pdf["avg_order_value"]):
            axes[2].text(
                bar.get_x() + bar.get_width()/2,
                bar.get_height() * 1.01,
                f"₹{val:,.0f}",
                ha="center", fontsize=9)

        plt.tight_layout()
        save_chart(fig, "07_device_breakdown.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# ANALYSIS 8 — TOP 10 CITIES
# ══════════════════════════════════════════════════════════════════
def analyse_top_cities(spark):
    print("\n📊 Analysis 8: Top 10 Cities by Revenue")
    try:
        df = spark.sql("""
            SELECT
                city,
                region,
                COUNT(order_id)             AS total_orders,
                ROUND(SUM(final_amount), 2) AS total_revenue,
                ROUND(AVG(final_amount), 2) AS avg_order_value
            FROM orders
            GROUP BY city, region
            ORDER BY total_revenue DESC
            LIMIT 10
        """)
        df.show()
        pdf = df.toPandas()
        bar_colors = [REGION_COLORS.get(r, COLORS["secondary"])
                      for r in pdf["region"]]

        fig, axes = plt.subplots(1, 2, figsize=(16, 7))
        fig.suptitle("Top 10 Cities by Revenue",
                     fontsize=15, fontweight="bold",
                     color=COLORS["primary"])

        # Horizontal bar
        bars = axes[0].barh(
            range(len(pdf)), pdf["total_revenue"],
            color=bar_colors, edgecolor="white"
        )
        axes[0].set_yticks(range(len(pdf)))
        axes[0].set_yticklabels(
            [f"{c} ({r})"
             for c, r in zip(pdf["city"], pdf["region"])],
            fontsize=9
        )
        axes[0].invert_yaxis()
        axes[0].set_title("Revenue Ranking",
                          fontsize=11, color=COLORS["primary"])
        axes[0].xaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"₹{v/100000:.1f}L"))
        for bar, val in zip(bars, pdf["total_revenue"]):
            axes[0].text(
                bar.get_width() * 1.01,
                bar.get_y() + bar.get_height()/2,
                f"₹{val/100000:.1f}L",
                va="center", fontsize=8)

        # Scatter — orders vs avg value
        scatter_colors = [
            REGION_COLORS.get(r, COLORS["secondary"])
            for r in pdf["region"]
        ]
        axes[1].scatter(
            pdf["total_orders"],
            pdf["avg_order_value"],
            c=scatter_colors,
            s=pdf["total_revenue"] / 5000,
            alpha=0.8,
            edgecolors="white",
            linewidth=1.5
        )
        for _, row in pdf.iterrows():
            axes[1].annotate(
                row["city"],
                (row["total_orders"], row["avg_order_value"]),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8
            )
        axes[1].set_xlabel("Total Orders")
        axes[1].set_ylabel("Avg Order Value (₹)")
        axes[1].set_title(
            "Orders vs Avg Value\n(bubble = revenue)",
            fontsize=11, color=COLORS["primary"])
        axes[1].yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"₹{v:,.0f}"))

        from matplotlib.patches import Patch
        legend_patches = [
            Patch(color=v, label=k)
            for k, v in REGION_COLORS.items()
        ]
        axes[1].legend(handles=legend_patches,
                       title="Region", fontsize=8)

        plt.tight_layout()
        save_chart(fig, "08_top_cities.png")
        return pdf

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# EXCEL EXPORT — ALL SHEETS
# ══════════════════════════════════════════════════════════════════
def export_to_excel(spark):
    print("\n" + "="*60)
    print("  📊 Exporting to Excel")
    print("="*60)

    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as writer:

        def write_sheet(query, sheet_name, label, limit=None):
            """Run a SQL query and write results to Excel sheet."""
            try:
                sql = query if not limit else \
                      query + f" LIMIT {limit}"
                df = spark.sql(sql).toPandas()
                df.to_excel(writer,
                            sheet_name=sheet_name,
                            index=False)
                print(f"   ✅ {label}: {len(df)} rows")
            except Exception as e:
                print(f"   ⚠️  {label} skipped: {e}")

        # Sheet 1 — Orders Sample
        write_sheet("""
            SELECT order_id, customer_name, customer_email,
                   city, region,
                   ROUND(final_amount, 2) AS final_amount,
                   num_items, device, value_segment,
                   is_high_value, is_discounted,
                   ROUND(discount, 2) AS discount,
                   order_hour, date
            FROM orders
            ORDER BY date DESC, final_amount DESC
        """, "Orders", "Orders Sample", limit=1000)

        # Sheet 2 — Revenue by Region
        write_sheet("""
            SELECT region,
                   COUNT(order_id)                 AS total_orders,
                   ROUND(SUM(final_amount), 2)     AS total_revenue,
                   ROUND(AVG(final_amount), 2)     AS avg_order_value,
                   ROUND(SUM(discount), 2)         AS total_discount,
                   SUM(CASE WHEN is_high_value = true
                       THEN 1 ELSE 0 END)          AS high_value_orders
            FROM orders
            GROUP BY region
            ORDER BY total_revenue DESC
        """, "Revenue by Region", "Revenue by Region")

        # Sheet 3 — Daily Sales Trend
        write_sheet("""
            SELECT date,
                   COUNT(order_id)             AS total_orders,
                   ROUND(SUM(final_amount), 2) AS daily_revenue,
                   ROUND(AVG(final_amount), 2) AS avg_order_value,
                   SUM(CASE WHEN discount > 0
                       THEN 1 ELSE 0 END)      AS discounted_orders,
                   SUM(CASE WHEN is_high_value = true
                       THEN 1 ELSE 0 END)      AS high_value_orders
            FROM orders
            GROUP BY date
            ORDER BY date ASC
        """, "Daily Sales Trend", "Daily Sales Trend")

        # Sheet 4 — Payment Analysis
        write_sheet("""
            SELECT payment_method,
                   COUNT(payment_id)                       AS total_transactions,
                   SUM(CASE WHEN is_successful = true
                       THEN 1 ELSE 0 END)                  AS successful,
                   SUM(CASE WHEN is_successful = false
                       THEN 1 ELSE 0 END)                  AS failed,
                   ROUND(
                       SUM(CASE WHEN is_successful = true
                           THEN 1 ELSE 0 END) * 100.0
                       / COUNT(payment_id), 2)             AS success_rate_pct,
                   ROUND(AVG(amount), 2)                   AS avg_transaction
            FROM payments
            GROUP BY payment_method
            ORDER BY total_transactions DESC
        """, "Payment Analysis", "Payment Analysis")

        # Sheet 5 — Value Segments
        write_sheet("""
            SELECT value_segment,
                   COUNT(order_id)             AS total_orders,
                   ROUND(SUM(final_amount), 2) AS total_revenue,
                   ROUND(AVG(final_amount), 2) AS avg_order_value,
                   ROUND(AVG(num_items), 2)    AS avg_items_per_order,
                   ROUND(SUM(discount), 2)     AS total_discount
            FROM orders
            GROUP BY value_segment
            ORDER BY total_revenue DESC
        """, "Value Segments", "Value Segments")

        # Sheet 6 — Peak Hours
        write_sheet("""
            SELECT order_hour        AS hour_of_day,
                   COUNT(order_id)   AS total_orders,
                   ROUND(SUM(final_amount), 2) AS total_revenue,
                   ROUND(AVG(final_amount), 2) AS avg_order_value,
                   SUM(CASE WHEN order_day_of_week IN (1,7)
                       THEN 1 ELSE 0 END)      AS weekend_orders,
                   SUM(CASE WHEN order_day_of_week NOT IN (1,7)
                       THEN 1 ELSE 0 END)      AS weekday_orders
            FROM orders
            GROUP BY order_hour
            ORDER BY order_hour ASC
        """, "Peak Hours", "Peak Hours")

        # Sheet 7 — Top Cities
        write_sheet("""
            SELECT city, region,
                   COUNT(order_id)             AS total_orders,
                   ROUND(SUM(final_amount), 2) AS total_revenue,
                   ROUND(AVG(final_amount), 2) AS avg_order_value,
                   SUM(CASE WHEN is_high_value = true
                       THEN 1 ELSE 0 END)      AS high_value_orders
            FROM orders
            GROUP BY city, region
            ORDER BY total_revenue DESC
            LIMIT 20
        """, "Top Cities", "Top Cities")

        # Sheet 8 — User Event Funnel
        write_sheet("""
            SELECT event_type,
                   COUNT(event_id)                 AS total_events,
                   COUNT(DISTINCT customer_id)     AS unique_customers,
                   SUM(CASE WHEN device = 'MOBILE'
                       THEN 1 ELSE 0 END)          AS mobile_events,
                   SUM(CASE WHEN device = 'DESKTOP'
                       THEN 1 ELSE 0 END)          AS desktop_events,
                   SUM(CASE WHEN device = 'TABLET'
                       THEN 1 ELSE 0 END)          AS tablet_events
            FROM user_events
            GROUP BY event_type
            ORDER BY total_events DESC
        """, "User Event Funnel", "User Event Funnel")

        # Sheet 9 — Customer RFM (if available)
        write_sheet("""
            SELECT customer_id, customer_name, customer_email,
                   last_order_date, days_since_order,
                   total_orders,
                   ROUND(total_revenue, 2)     AS total_revenue,
                   ROUND(avg_order_value, 2)   AS avg_order_value,
                   recency_score, frequency_score,
                   monetary_score, rfm_segment,
                   computed_date
            FROM customer_rfm
            ORDER BY total_revenue DESC
        """, "Customer RFM", "Customer RFM")

        # Sheet 10 — Payments Sample
        write_sheet("""
            SELECT payment_id, order_id, customer_id,
                   ROUND(amount, 2) AS amount,
                   payment_method, status, gateway,
                   is_successful, payment_hour, date
            FROM payments
            ORDER BY date DESC
        """, "Payments Sample", "Payments Sample", limit=1000)

    print(f"\n✅ Excel file saved:")
    print(f"   📁 {EXCEL_PATH}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    print("\n" + "="*60)
    print("  🏪 E-COMMERCE ANALYTICS PLATFORM")
    print("  Phase 4: Analytics + Charts + Excel Export")
    print("="*60)

    # Start Spark
    print("\n📡 Starting Spark...")
    spark = create_spark()
    print(f"   Spark version: {spark.version}")

    # Load tables
    load_tables(spark)

    # Run all analyses
    print("\n" + "="*60)
    print("  Running Analytics + Generating Charts")
    print("="*60)

    analyse_revenue_by_region(spark)
    analyse_sales_trend(spark)
    analyse_peak_hours(spark)
    analyse_value_segments(spark)
    analyse_payments(spark)
    analyse_rfm(spark)
    analyse_devices(spark)
    analyse_top_cities(spark)

    # Export to Excel
    export_to_excel(spark)

    # Final summary
    print("\n" + "="*60)
    print("  ✅ ALL DONE")
    print("="*60)

    charts = [f for f in os.listdir(CHARTS_PATH)
              if f.endswith(".png")]
    print(f"\n📊 Charts generated: {len(charts)}")
    for c in sorted(charts):
        print(f"   ✅ {c}")

    print(f"\n📗 Excel file:")
    print(f"   ✅ Ecommerce_Analytics_Report.xlsx")
    print(f"      Location: {EXCEL_PATH}")

    spark.stop()
    print("\n🎉 Phase 4 Complete!")


if __name__ == "__main__":
    main()
-- ╔══════════════════════════════════════════════════════════════╗
-- ║  E-COMMERCE ANALYTICS PLATFORM — HIVE TABLE DEFINITIONS     ║
-- ║  Phase 3: Creates SQL tables on top of Delta Lake           ║
-- ║                                                              ║
-- ║  NOTE: Using 'dt' instead of 'date' because                 ║
-- ║  'date' is a reserved keyword in Hive 4.0                   ║
-- ╚══════════════════════════════════════════════════════════════╝


-- ─── CREATE DATABASE ───────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ecommerce
COMMENT 'E-Commerce Analytics Platform — all tables'
LOCATION '/opt/delta-lake/hive-db';

USE ecommerce;


-- ═══════════════════════════════════════════════════════════════
-- SECTION 1: CORE TABLES
-- (External tables pointing to Delta Lake curated layer)
-- ═══════════════════════════════════════════════════════════════


-- ─── ORDERS TABLE ──────────────────────────────────────────────
DROP TABLE IF EXISTS orders;

CREATE EXTERNAL TABLE orders (
    order_id            STRING,
    customer_id         STRING,
    customer_name       STRING,
    customer_email      STRING,
    region              STRING,
    city                STRING,
    num_items           INT,
    subtotal            DOUBLE,
    discount            DOUBLE,
    final_amount        DOUBLE,
    device              STRING,
    status              STRING,
    order_timestamp     STRING,
    hour                INT,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    kafka_timestamp     TIMESTAMP,
    event_timestamp     TIMESTAMP,
    is_high_value       BOOLEAN,
    is_discounted       BOOLEAN,
    order_day_of_week   INT,
    order_hour          INT,
    value_segment       STRING,
    processed_at        TIMESTAMP,
    pipeline_version    STRING
)
COMMENT 'Curated orders — cleaned and enriched from Kafka via Spark'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/opt/delta-lake/curated/orders'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Add partitions — dt = Hive column name, date= = actual folder Spark created
ALTER TABLE orders ADD IF NOT EXISTS
PARTITION (dt='2026-05-16')
LOCATION '/opt/delta-lake/curated/orders/date=2026-05-16';

ALTER TABLE orders ADD IF NOT EXISTS
PARTITION (dt='2026-05-17')
LOCATION '/opt/delta-lake/curated/orders/date=2026-05-17';

ALTER TABLE orders ADD IF NOT EXISTS
PARTITION (dt='2026-05-18')
LOCATION '/opt/delta-lake/curated/orders/date=2026-05-18';

ALTER TABLE orders ADD IF NOT EXISTS
PARTITION (dt='2026-05-19')
LOCATION '/opt/delta-lake/curated/orders/date=2026-05-19';

ALTER TABLE orders ADD IF NOT EXISTS
PARTITION (dt='2026-05-20')
LOCATION '/opt/delta-lake/curated/orders/date=2026-05-20';


-- ─── PAYMENTS TABLE ────────────────────────────────────────────
DROP TABLE IF EXISTS payments;

CREATE EXTERNAL TABLE payments (
    payment_id          STRING,
    order_id            STRING,
    customer_id         STRING,
    amount              DOUBLE,
    payment_method      STRING,
    status              STRING,
    gateway             STRING,
    transaction_id      STRING,
    payment_timestamp   STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    is_successful       BOOLEAN,
    event_timestamp     TIMESTAMP,
    payment_hour        INT,
    processed_at        TIMESTAMP,
    pipeline_version    STRING
)
COMMENT 'Curated payments — cleaned and enriched from Kafka via Spark'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/opt/delta-lake/curated/payments'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

ALTER TABLE payments ADD IF NOT EXISTS
PARTITION (dt='2026-05-16')
LOCATION '/opt/delta-lake/curated/payments/date=2026-05-16';

ALTER TABLE payments ADD IF NOT EXISTS
PARTITION (dt='2026-05-17')
LOCATION '/opt/delta-lake/curated/payments/date=2026-05-17';

ALTER TABLE payments ADD IF NOT EXISTS
PARTITION (dt='2026-05-18')
LOCATION '/opt/delta-lake/curated/payments/date=2026-05-18';

ALTER TABLE payments ADD IF NOT EXISTS
PARTITION (dt='2026-05-19')
LOCATION '/opt/delta-lake/curated/payments/date=2026-05-19';

ALTER TABLE payments ADD IF NOT EXISTS
PARTITION (dt='2026-05-20')
LOCATION '/opt/delta-lake/curated/payments/date=2026-05-20';


-- ─── USER EVENTS TABLE ─────────────────────────────────────────
DROP TABLE IF EXISTS user_events;

CREATE EXTERNAL TABLE user_events (
    event_id            STRING,
    customer_id         STRING,
    event_type          STRING,
    product_id          STRING,
    product_name        STRING,
    category            STRING,
    search_query        STRING,
    device              STRING,
    session_id          STRING,
    page_url            STRING,
    event_time          STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    is_product_event    BOOLEAN,
    is_search_event     BOOLEAN,
    event_timestamp     TIMESTAMP,
    event_hour          INT,
    processed_at        TIMESTAMP,
    pipeline_version    STRING
)
COMMENT 'Curated user events — clicks, searches, add to cart'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/opt/delta-lake/curated/user_events'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

ALTER TABLE user_events ADD IF NOT EXISTS
PARTITION (dt='2026-05-16')
LOCATION '/opt/delta-lake/curated/user_events/date=2026-05-16';

ALTER TABLE user_events ADD IF NOT EXISTS
PARTITION (dt='2026-05-17')
LOCATION '/opt/delta-lake/curated/user_events/date=2026-05-17';

ALTER TABLE user_events ADD IF NOT EXISTS
PARTITION (dt='2026-05-18')
LOCATION '/opt/delta-lake/curated/user_events/date=2026-05-18';

ALTER TABLE user_events ADD IF NOT EXISTS
PARTITION (dt='2026-05-19')
LOCATION '/opt/delta-lake/curated/user_events/date=2026-05-19';

ALTER TABLE user_events ADD IF NOT EXISTS
PARTITION (dt='2026-05-20')
LOCATION '/opt/delta-lake/curated/user_events/date=2026-05-20';


-- ═══════════════════════════════════════════════════════════════
-- SECTION 2: SUMMARY TABLES
-- (Managed tables — populated by Airflow nightly DAG)
-- ═══════════════════════════════════════════════════════════════


-- ─── DAILY SALES SUMMARY ───────────────────────────────────────
DROP TABLE IF EXISTS daily_sales_summary;

CREATE TABLE daily_sales_summary (
    sale_date           STRING,
    total_orders        INT,
    total_revenue       DOUBLE,
    avg_order_value     DOUBLE,
    total_discount      DOUBLE,
    high_value_orders   INT,
    cancelled_orders    INT,
    region              STRING,
    mobile_orders       INT,
    desktop_orders      INT,
    computed_date       STRING
)
COMMENT 'Daily sales aggregates — computed by Airflow nightly'
STORED AS PARQUET
LOCATION '/opt/delta-lake/summaries/daily_sales';


-- ─── CUSTOMER RFM SCORES ───────────────────────────────────────
DROP TABLE IF EXISTS customer_rfm;

CREATE TABLE customer_rfm (
    customer_id         STRING,
    customer_name       STRING,
    customer_email      STRING,
    last_order_date     STRING,
    days_since_order    INT,
    total_orders        INT,
    total_revenue       DOUBLE,
    avg_order_value     DOUBLE,
    recency_score       INT,
    frequency_score     INT,
    monetary_score      INT,
    rfm_segment         STRING,
    computed_date       STRING
)
COMMENT 'Customer RFM scores — Recency Frequency Monetary analysis'
STORED AS PARQUET
LOCATION '/opt/delta-lake/summaries/customer_rfm';


-- ─── CHURN CANDIDATES ──────────────────────────────────────────
DROP TABLE IF EXISTS churn_candidates;

CREATE TABLE churn_candidates (
    customer_id          STRING,
    customer_name        STRING,
    customer_email       STRING,
    last_order_date      STRING,
    days_since_order     INT,
    total_lifetime_value DOUBLE,
    churn_risk           STRING,
    computed_date        STRING
)
COMMENT 'Churn risk customers — identified by Airflow nightly'
STORED AS PARQUET
LOCATION '/opt/delta-lake/summaries/churn_candidates';


-- ─── PAYMENT ANALYSIS ──────────────────────────────────────────
DROP TABLE IF EXISTS payment_analysis;

CREATE TABLE payment_analysis (
    analysis_date       STRING,
    payment_method      STRING,
    total_transactions  INT,
    successful          INT,
    failed              INT,
    success_rate        DOUBLE,
    total_amount        DOUBLE,
    computed_date       STRING
)
COMMENT 'Payment method performance — computed by Airflow nightly'
STORED AS PARQUET
LOCATION '/opt/delta-lake/summaries/payment_analysis';


-- ═══════════════════════════════════════════════════════════════
-- SECTION 3: VERIFY EVERYTHING CREATED CORRECTLY
-- ═══════════════════════════════════════════════════════════════
SHOW DATABASES;
SHOW TABLES IN ecommerce;
SHOW PARTITIONS orders;
SHOW PARTITIONS payments;
SHOW PARTITIONS user_events;
DESCRIBE ecommerce.orders;
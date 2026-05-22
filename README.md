# 🛒 Real-Time E-Commerce Analytics Platform

A production-grade end-to-end data platform combining **Data Engineering**
and **Data Analytics** — built with the most in-demand Big Data tools of 2026.

---

## 🏗️ Architecture
Python Producer (Kafka)
│
▼
Apache Kafka (3 topics: orders, payments, user_events)
│
▼
Apache Spark Structured Streaming
│
├──► Delta Lake RAW Layer      (exact copy from Kafka)
│
└──► Delta Lake CURATED Layer  (cleaned + enriched + partitioned)
│
▼
Apache Hive External Tables  ← Phase 3 ✅
(SQL interface on Delta Lake)
│
▼
Apache Airflow DAG           ← Phase 3 ✅
(nightly: RFM + Churn + Sales aggregations)
│
▼
Analytics (PySpark RFM, Churn)  ← Phase 4
│
▼
Power BI / Superset Dashboard   ← Phase 4

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Data Ingestion | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Storage | Delta Lake (Lakehouse — Raw + Curated layers) |
| SQL Interface | Apache Hive 4.0 + HiveServer2 |
| Metastore | Apache Hive Metastore + PostgreSQL 13 |
| Orchestration | Apache Airflow 2.8.1 |
| Analytics | PySpark, Python (Pandas, Matplotlib) |
| Visualisation | Power BI / Apache Superset |
| Infrastructure | Docker, Docker Compose |

---

## 📦 Project Phases

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Kafka data ingestion — Producer & Consumer | ✅ Complete |
| Phase 2 | Spark Structured Streaming + Delta Lake Lakehouse | ✅ Complete |
| Phase 3 | Hive SQL tables + Airflow nightly pipeline | ✅ Complete |
| Phase 4 | Analytics + Dashboard | 🔄 In Progress |
| Phase 5 | Cloud deployment | ⏳ Upcoming |

---

## 📸 Screenshots

### Phase 1 — Kafka Producer: Live Orders Streaming
![Kafka Producer](screenshots/01-kafka-producer-running.png)

### Phase 1 — Kafka Consumer: Reading Messages
![Kafka Consumer](screenshots/02-kafka-consumer-output.png)

### Phase 1 — Kafka UI: Topics Dashboard
![Kafka UI](screenshots/04-kafka-ui-dashboard.png)

### Phase 2 — Spark Streaming: Live Batch Processing
![Spark Streaming](screenshots/05-spark-streaming-batches.png)

### Phase 2 — Delta Lake: Raw and Curated Folders
![Delta Lake](screenshots/06-delta-lake-folders.png)

### Phase 2 — Spark UI: Jobs and Streaming Dashboard
![Spark UI](screenshots/07-spark-ui-dashboard.png)

### Phase 3 — Hive Tables: SQL on Delta Lake
![Hive Tables](screenshots/08-hive-tables.png)

### Phase 3 — Airflow DAG: Nightly Pipeline
![Airflow DAG](screenshots/09-airflow-dag.png)

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop installed and running
- Python 3.8+

### Step 1 — Start all infrastructure
```bash
docker compose up -d
docker compose ps
```

Expected containers:
zookeeper       Up
kafka           Up
kafka-ui        Up
spark           Up
hive-postgres   Up
hive-metastore  Up
hiveserver2     Up
airflow         Up

---

## 🚀 Phase 1 — Kafka

### Start Producer (Terminal 1)
```bash
pip install kafka-python faker
cd kafka
python producer.py
```

### Start Consumer (Terminal 2)
```bash
cd kafka
python consumer.py
```

### View Kafka UI
Open browser: **http://localhost:8080**

---

## 🚀 Phase 2 — Spark + Delta Lake

### Install packages inside Spark container
```bash
docker exec -it spark bash
pip install pyspark==3.4.0 delta-spark==2.4.0 kafka-python faker
```

### Run Spark Streaming
```bash
cd /opt/spark-apps
python streaming_job.py
```

### Verify Delta Lake (after 30 seconds)
```bash
python verify_delta.py
```

### View Spark UI
Open browser: **http://localhost:4040**

---

## 🚀 Phase 3 — Hive + Airflow

### Create Hive Tables
```bash
docker exec -it hiveserver2 bash
beeline -u jdbc:hive2://localhost:10000 -f /opt/hive-scripts/create_tables.sql
```

### Run Sample Queries
```bash
# Inside beeline
beeline -u jdbc:hive2://localhost:10000
USE ecommerce;
SELECT region, COUNT(*) as orders, ROUND(SUM(final_amount),2) as revenue
FROM orders GROUP BY region ORDER BY revenue DESC;
```

### View Airflow UI
Open browser: **http://localhost:8088**
Username: admin
Password: admin123

Trigger the DAG manually → **ecommerce_nightly_pipeline** → ▶️

---

## 📊 What the Pipeline Does

### Phase 1 — Kafka Ingestion
- Generates realistic Indian e-commerce orders (15 products, Indian cities, rupee prices)
- Streams payments — UPI, Credit Card, Net Banking (90% success rate)
- Captures user events — page views, clicks, add to cart, searches
- All linked by customer_id — simulates real user sessions

### Phase 2 — Spark Processing
**Cleaning:**
- Remove null order_id, customer_id, amount
- Filter zero and negative amounts
- Standardise text to UPPERCASE and trim spaces
- Round money values to 2 decimal places

**Enrichment:**
- `is_high_value` — True for orders above ₹50,000
- `value_segment` — PREMIUM / HIGH / MEDIUM / LOW
- `order_hour` — peak time analysis
- `order_day_of_week` — weekend vs weekday patterns
- `processed_at` — data lineage timestamp

**Delta Lake Lakehouse:**
- RAW layer — exact Kafka copy, source of truth
- CURATED layer — cleaned, enriched, partitioned by date

### Phase 3 — Hive + Airflow

**Hive Tables (External — pointing to Delta Lake):**
- `orders` — partitioned by dt, STORED AS PARQUET
- `payments` — partitioned by dt, STORED AS PARQUET
- `user_events` — partitioned by dt, STORED AS PARQUET

**Hive Summary Tables (Managed — written by Airflow):**
- `daily_sales_summary` — revenue, orders by region per day
- `customer_rfm` — Recency Frequency Monetary scores
- `churn_candidates` — customers inactive 30/60/90+ days
- `payment_analysis` — payment method success rates

**Airflow DAG — runs nightly at midnight:**
pipeline_start
│
compute_daily_sales
│
┌─────┼──────────────┐
▼     ▼              ▼
RFM  Churn     Payment Analysis
│     │              │
└─────┼──────────────┘
▼
pipeline_complete

---

## 🗂️ Repository Structure
ecommerce-analytics-platform/
│
├── docker-compose.yml       # Full infrastructure — all 8 services
├── README.md
├── .gitignore
├── screenshots/             # Portfolio screenshots
│
├── kafka/                   # Phase 1
│   ├── producer.py          # Generates and streams fake orders
│   └── consumer.py          # Reads and displays messages
│
├── spark/                   # Phase 2
│   ├── streaming_job.py     # Spark Structured Streaming pipeline
│   └── verify_delta.py      # Delta Lake verification
│
├── hive/                    # Phase 3
│   ├── create_tables.sql    # Creates all 7 Hive tables
│   └── sample_queries.sql   # 10 business queries
│
├── airflow/                 # Phase 3
│   └── dags/
│       └── ecommerce_pipeline.py  # Nightly batch DAG
│
├── delta-lake/              # Auto-created by Spark
│   ├── raw/                 # Exact Kafka copy
│   ├── curated/             # Cleaned + enriched + partitioned
│   └── summaries/           # Airflow aggregations
│
└── analytics/               # Phase 4 (coming)

---

## 👤 Author

**Rakshith CS**
Data Engineering Enthusiast
[GitHub](https://github.com/rakshithcs228-tech) | [LinkedIn](https://linkedin.com/in/yourprofile)

# 🛒 Real-Time E-Commerce Analytics Platform

A production-grade end-to-end data platform combining **Data Engineering**
and **Data Analytics** — built with the most in-demand Big Data tools of 2026.

---

## 🏗️ Architecture
Python Producer
│
▼
Apache Kafka ──────────────────────────────────────────────
(orders | payments | user_events)                         │
│                                                    │
▼                                                    │
Apache Spark Structured Streaming                         │
│                                                    │
├──► Delta Lake RAW Layer                           │
│    (exact copy from Kafka)                        │
│                                                    │
└──► Delta Lake CURATED Layer                       │
(cleaned + enriched + partitioned)              │
│                                      │
▼                                      │
Apache Hive 4.0                                │
(SQL interface on Delta Lake)                   │
│                                      │
▼                                      │
Apache Airflow 2.8.1                           │
(nightly: RFM + Churn + Sales + Payments)      │
│                                      │
▼                                      │
PySpark Analytics                              │
(8 analyses + 8 charts + Excel export)         │
│                                      │
▼                                      │
Apache Superset Dashboard ◄─────────────────────
(live BI dashboard — auto-refresh 30s)

---

## 🔧 Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Data Ingestion | Apache Kafka | 7.5.0 |
| Stream Processing | Apache Spark Structured Streaming | 3.4.0 |
| Storage | Delta Lake (Lakehouse) | 2.4.0 |
| SQL Interface | Apache Hive + HiveServer2 | 4.0.0 |
| Metastore Backend | PostgreSQL | 13 |
| Orchestration | Apache Airflow | 2.8.1 |
| Analytics | PySpark + Spark SQL | 3.4.0 |
| Visualisation | Matplotlib + Seaborn | latest |
| Excel Export | Pandas + openpyxl | latest |
| BI Dashboard | Apache Superset | 3.0.0 |
| Infrastructure | Docker + Docker Compose | latest |

---

## 📦 Project Phases

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Kafka ingestion — Producer & Consumer | ✅ Complete |
| Phase 2 | Spark Structured Streaming + Delta Lake Lakehouse | ✅ Complete |
| Phase 3 | Hive SQL tables + Airflow nightly pipeline | ✅ Complete |
| Phase 4 | PySpark Analytics + Charts + Excel + Superset Dashboard | ✅ Complete |
| Phase 5 | Cloud Deployment (AWS/Azure) | ⏳ Upcoming |

---

## 📸 Screenshots

### Phase 1 — Kafka Producer: Live Orders Streaming
![Kafka Producer](screenshots/01-kafka-producer-running.png)

### Phase 1 — Kafka Consumer: Reading Messages in Real Time
![Kafka Consumer](screenshots/02-kafka-consumer-output.png)

### Phase 1 — Kafka UI: Topics Dashboard
![Kafka UI](screenshots/03-kafka-ui-dashboard.png)

### Phase 2 — Spark Streaming: Live Batch Processing
![Spark Streaming](screenshots/04-spark-streaming-batches.png)

### Phase 2 — Delta Lake: Raw and Curated Folders
![Delta Lake](screenshots/05-delta-lake-folders.png)

### Phase 2 — Spark UI: Jobs and Streaming Dashboard
![Spark UI](screenshots/06-spark-ui-dashboard.png)

### Phase 3 — Hive Tables: SQL on Delta Lake
![Hive Tables](screenshots/07-hive-tables.png)

### Phase 3 — Airflow DAG: Pipeline Graph
![Airflow DAG](screenshots/08-airflow-dag.png)

### Phase 4 — Analytics Charts: All 8 Charts Generated
![Charts Folder](screenshots/09-analytics-charts-folder.png)

### Phase 4 — Sample Chart: Revenue and Peak Hours Analysis
![Sample Chart](screenshots/10-analytics-chart-sample.png)

### Phase 4 — Excel Report: 10-Sheet Business Report
![Excel Report](screenshots/11-excel-report.png)

### Phase 4 — Airflow DAG: All Tasks Succeeded
![Airflow Success](screenshots/12-airflow-dag-success.png)

### Phase 4 — Superset: Live Business Dashboard
![Superset Dashboard](screenshots/13-superset-dashboard.png)

### Phase 4 — Docker: All 9 Containers Running
![All Containers](screenshots/14-all-containers-running.png)

---

## 🚀 How to Run — Complete Setup

### Prerequisites
```bash
# Required software
Docker Desktop    → https://www.docker.com/products/docker-desktop
Python 3.8+       → https://www.python.org/downloads
Java 11 or 17     → https://adoptium.net
Git               → https://git-scm.com
```

### Step 1 — Clone and Start Infrastructure
```bash
git clone https://github.com/rakshithcs228-tech/ecommerce-analytics-platform.git
cd ecommerce-analytics-platform

# Build custom Airflow image (first time only — takes 3-5 minutes)
docker compose build airflow

# Start all 9 containers
docker compose up -d

# Verify all containers are running
docker compose ps
```

### Step 2 — Install Python Dependencies
```bash
pip install kafka-python faker pyspark==3.4.0 delta-spark==2.4.0
pip install matplotlib seaborn pandas openpyxl
```

---

## 🚀 Phase 1 — Kafka Streaming

```bash
# Terminal 1 — Start Producer
cd kafka
python producer.py

# Terminal 2 — Start Consumer
cd kafka
python consumer.py

# View Kafka UI → http://localhost:8080
```

---

## 🚀 Phase 2 — Spark + Delta Lake

```bash
# Open Spark container
docker exec -it spark bash
pip install pyspark==3.4.0 delta-spark==2.4.0 kafka-python

# Run streaming job (keep running)
cd /opt/spark-apps
python streaming_job.py

# Verify Delta Lake (new terminal)
docker exec -it spark bash
cd /opt/spark-apps
python verify_delta.py

# View Spark UI → http://localhost:4040
```

---

## 🚀 Phase 3 — Hive + Airflow

```bash
# Create Hive tables
docker exec -it hiveserver2 bash
beeline -u jdbc:hive2://localhost:10000 -f /opt/hive-scripts/create_tables.sql

# Run sample queries
USE ecommerce;
SELECT region, COUNT(*) AS orders,
       ROUND(SUM(final_amount),2) AS revenue
FROM orders GROUP BY region ORDER BY revenue DESC;

# View Airflow UI → http://localhost:8088 (admin/admin123)
# Trigger DAG → ecommerce_nightly_pipeline → ▶ Trigger DAG
```

---

## 🚀 Phase 4 — Analytics + Dashboard

```bash
# Run all analytics (8 charts + Excel export)
cd analytics
python analytics_job.py

# Charts saved to: analytics/charts/
# Excel saved to:  analytics/Ecommerce_Analytics_Report.xlsx

# View Superset Dashboard → http://localhost:8089 (admin/admin123)
# Connect to Hive: hive://hive@hiveserver2:10000/ecommerce
```

---

## 📊 Key Business Insights

| Metric | Finding |
|---|---|
| Top Region | South — ₹21.8Cr revenue |
| Biggest Segment | PREMIUM — 47% of all orders, avg ₹2.76L per order |
| Best Payment Method | UPI — 90.35% success rate |
| Top City | Kochi — ₹6.17Cr revenue |
| Device Split | Almost equal — Mobile 34%, Desktop 34%, Tablet 32% |
| Peak Hour | Orders concentrated in business hours |
| Data Volume | 7,259 orders · 7,253 payments · 25,413 user events |

---

## 📊 Analytics Generated

### 8 Charts
| Chart | Analysis |
|---|---|
| 01_revenue_by_region | Revenue and order distribution across 5 regions |
| 02_sales_trend | Daily revenue trend and order volume over time |
| 03_peak_hours | Heatmap of orders by hour and day of week |
| 04_value_segments | PREMIUM/HIGH/MEDIUM/LOW customer breakdown |
| 05_payment_methods | Success rates and volumes per payment method |
| 06_rfm_distribution | Customer segments — Champions to Lost Customers |
| 07_device_breakdown | Mobile vs Desktop vs Tablet comparison |
| 08_top_cities | Top 10 cities ranked by revenue |

### 10-Sheet Excel Report
Sheet 1:  Orders Sample (1,000 records with enriched columns)
Sheet 2:  Revenue by Region
Sheet 3:  Daily Sales Trend
Sheet 4:  Payment Method Analysis
Sheet 5:  Value Segment Distribution
Sheet 6:  Peak Hours (24 hours × weekend/weekday)
Sheet 7:  Top 20 Cities
Sheet 8:  User Event Funnel
Sheet 9:  Customer RFM Scores
Sheet 10: Payments Sample

---

## 🗂️ Repository Structure
ecommerce-analytics-platform/
│
├── Dockerfile.airflow       # Custom Airflow image — Java 17 + PySpark baked in
├── docker-compose.yml       # All 9 services — Kafka + Spark + Hive + Airflow + Superset
├── README.md
├── .gitignore
├── screenshots/             # 14 portfolio screenshots
│
├── kafka/                   # Phase 1
│   ├── producer.py          # Streams realistic Indian e-commerce data to Kafka
│   └── consumer.py          # Reads and displays messages with colour coding
│
├── spark/                   # Phase 2
│   ├── streaming_job.py     # Spark Structured Streaming — 3 parallel pipelines
│   └── verify_delta.py      # Queries Delta Lake to verify data quality
│
├── hive/                    # Phase 3
│   ├── create_tables.sql    # 7 Hive tables — external on Delta Lake
│   └── sample_queries.sql   # 10 business queries for demonstration
│
├── airflow/                 # Phase 3
│   └── dags/
│       └── ecommerce_pipeline.py  # Nightly DAG — RFM, Churn, Sales, Payments
│
├── analytics/               # Phase 4
│   ├── analytics_job.py     # 8 PySpark analyses + chart generation + Excel export
│   ├── charts/              # 8 PNG chart files
│   └── Ecommerce_Analytics_Report.xlsx  # 10-sheet Excel business report
│
└── delta-lake/              # Auto-created by Spark — never committed to Git
├── raw/                 # Exact Kafka copy — source of truth
├── curated/             # Cleaned + enriched + partitioned by date
└── summaries/           # Airflow nightly aggregations

---

## 🐳 Infrastructure — 9 Docker Containers

| Container | Image | Port | Purpose |
|---|---|---|---|
| zookeeper | cp-zookeeper:7.5.0 | 2181 | Kafka coordinator |
| kafka | cp-kafka:7.5.0 | 9092 | Message broker |
| kafka-ui | kafka-ui:latest | 8080 | Kafka browser UI |
| spark | apache/spark:3.4.0 | 4040 | Stream processing |
| hive-postgres | postgres:13 | 5432 | Hive Metastore backend |
| hive-metastore | apache/hive:4.0.0 | 9083 | Table metadata |
| hiveserver2 | apache/hive:4.0.0 | 10000 | SQL/JDBC interface |
| airflow | Custom Dockerfile | 8088 | Pipeline scheduler |
| superset | apache/superset:3.0.0 | 8089 | BI dashboard |

---

## 👤 Author

**Rakshith M**
Data Engineering Portfolio Project — 2026
[GitHub](https://github.com/rakshithcs228-tech) | [LinkedIn](https://linkedin.com/in/MRakshith)

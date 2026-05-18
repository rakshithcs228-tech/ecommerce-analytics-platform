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
Apache Hive (SQL interface)     ← Phase 3
│
▼
Apache Airflow (orchestration)  ← Phase 3
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
| Batch Querying | Apache Hive + Spark SQL |
| Orchestration | Apache Airflow |
| Analytics | PySpark, Python (Pandas, Matplotlib) |
| Visualisation | Power BI / Apache Superset |
| Infrastructure | Docker, Docker Compose |

---

## 📦 Project Phases

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Kafka data ingestion — Producer & Consumer | ✅ Complete |
| Phase 2 | Spark Structured Streaming + Delta Lake Lakehouse | ✅ Complete |
| Phase 3 | Hive tables + Airflow batch pipeline | 🔄 In Progress |
| Phase 4 | Analytics + Dashboard | ⏳ Upcoming |
| Phase 5 | Docker + Cloud deployment | ⏳ Upcoming |

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

---

## 🚀 How to Run — Phase 1 (Kafka)

### Prerequisites
- Docker Desktop installed and running
- Python 3.8+

### Step 1 — Start infrastructure
```bash
docker compose up -d
```

### Step 2 — Install dependencies
```bash
pip install kafka-python faker
```

### Step 3 — Start Producer (Terminal 1)
```bash
cd kafka
python producer.py
```

### Step 4 — Start Consumer (Terminal 2)
```bash
cd kafka
python consumer.py
```

### Step 5 — View Kafka UI
Open browser: **http://localhost:8080**

---

## 🚀 How to Run — Phase 2 (Spark + Delta Lake)

### Step 1 — Start infrastructure
```bash
docker compose up -d
```

### Step 2 — Start Producer (Terminal 1)
```bash
cd kafka
python producer.py
```

### Step 3 — Open Spark container (Terminal 2)
```bash
docker exec -it spark bash
```

### Step 4 — Install and run inside container
```bash
pip install pyspark==3.4.0 delta-spark==2.4.0
cd /opt/spark-apps
python streaming_job.py
```

### Step 5 — Verify Delta Lake (Terminal 3 — after 30 seconds)
```bash
docker exec -it spark bash
cd /opt/spark-apps
python verify_delta.py
```

### Step 6 — View Spark UI
Open browser: **http://localhost:4040**

---

## 📊 What the Data Pipeline Does

### Phase 1 — Kafka Ingestion
- Generates realistic Indian e-commerce orders (15 products, Indian cities, rupee prices)
- Streams payments with UPI, Credit Card, Net Banking (90% success rate)
- Captures user events — page views, clicks, add to cart, searches
- All three linked by customer_id — simulates real user sessions

### Phase 2 — Spark Processing
**Cleaning applied to every record:**
- Remove null order_id, customer_id, amount
- Filter out zero and negative amounts
- Standardise text to UPPERCASE and trim spaces
- Round money values to 2 decimal places

**Enrichment added to curated layer:**
- `is_high_value` — True for orders above ₹50,000
- `value_segment` — PREMIUM / HIGH / MEDIUM / LOW
- `order_hour` — extracted hour for peak time analysis
- `order_day_of_week` — weekend vs weekday patterns
- `processed_at` — when Spark processed the record

**Delta Lake Lakehouse:**
- RAW layer — exact Kafka copy, source of truth
- CURATED layer — cleaned, enriched, partitioned by date

---

## 🗂️ Repository Structure
ecommerce-analytics-platform/
│
├── docker-compose.yml       # Full infrastructure — Kafka + Spark
├── README.md
├── .gitignore
├── screenshots/             # Visual proof — portfolio screenshots
│
├── kafka/                   # Phase 1
│   ├── producer.py          # Generates and streams fake orders
│   └── consumer.py          # Reads and displays messages
│
├── spark/                   # Phase 2
│   ├── streaming_job.py     # Spark Structured Streaming pipeline
│   └── verify_delta.py      # Delta Lake verification and queries
│
├── delta-lake/              # Auto-created by Spark
│   ├── raw/                 # Exact Kafka copy
│   └── curated/             # Cleaned + enriched + partitioned
│
├── hive/                    # Phase 3 (coming)
├── airflow/                 # Phase 3 (coming)
├── analytics/               # Phase 4 (coming)
└── dashboard/               # Phase 4 (coming)

---

## 👤 Author

**Rakshith CS**
Data Engineering Enthusiast
[GitHub](https://github.com/rakshithcs228-tech) | [LinkedIn](https://linkedin.com/in/yourprofile)


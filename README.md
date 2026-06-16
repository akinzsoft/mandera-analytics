# Mandera Analytics — Batch Data Engineering Pipeline

A production-style batch analytics pipeline that moves synthetic transactional data through a structured engineering stack — from source generation through to clean staging tables — orchestrated end-to-end with Apache Airflow.

---

## Architecture

```
Python + Faker
      │
      ▼
MongoDB Atlas          ← operational source (raw documents)
      │
      ├──────────────────────────────────┐
      ▼                                  ▼
MinIO Object Storage            PostgreSQL raw_transactions
(year/month/day partitions)     (landing zone)
                                         │
                                         ▼
                                PostgreSQL stg_transactions
                                (clean staging layer)
                                         │
                                         ▼
                                  raw_transactions
                                  (truncated — ready for next batch)

All steps orchestrated by Apache Airflow DAG
```

---

## Stack

| Tool | Version | Role |
|---|---|---|
| Python | 3.14 | Pipeline scripting |
| Faker | 40.23 | Synthetic data generation |
| MongoDB Atlas | 8.0.26 | Operational source database |
| MinIO | Latest | S3-compatible object storage |
| PostgreSQL | 15 | Raw landing zone and staging tables |
| Apache Airflow | 2.9.0 | DAG orchestration and scheduling |
| Docker Compose | 29.5.3 | Local service management |
| pymongo | 4.17 | MongoDB Python driver |
| psycopg2 | 2.9 | PostgreSQL Python driver |
| boto3 | 1.43 | S3-compatible MinIO client |

---

## Project Structure

```
mandera-analytics/
├── generator/
│   ├── data_generator.py          # Orchestrates all generation
│   ├── faker_customers.py         # Customer record generator
│   ├── faker_products.py          # Product record generator
│   └── faker_orders.py            # Order/transaction generator
│
├── extraction/
│   ├── extract_mongo_to_minio.py  # MongoDB → MinIO (JSON, partitioned)
│   └── extract_mongo_to_postgres.py # MongoDB → PostgreSQL raw tables
│
├── transformation/
│   ├── transform_orders.py        # Raw → staging with 7 transformations
│   ├── transform_customers.py     # Customer staging transformation
│   └── transform_products.py      # Product staging transformation
│
├── validation/
│   ├── validate_batch_counts.py   # Row count and variance checks
│   └── validate_data_quality.py   # Field-level quality validation
│
├── maintenance/
│   └── truncate_raw_tables.py     # Post-transformation raw table cleanup
│
├── airflow/
│   └── dags/
│       └── mandera_pipeline_dag.py # Airflow DAG definition
│
├── sql/
│   ├── create_raw_tables.sql      # DDL for raw landing tables
│   ├── create_staging_tables.sql  # DDL for staging tables
│   └── monitoring_tables.sql      # DDL for batch_log monitoring table
│
├── docs/
│   ├── data_dictionary.md         # Field definitions and data types
│   └── architecture.md            # Pipeline architecture documentation
│
├── docker-compose.yml             # PostgreSQL, MinIO and Airflow services
└── .env.example                   # Environment variable template
```

---

## Pipeline Flow

The Airflow DAG `mandera_batch_pipeline` runs four tasks in sequence:

```
generate_data >> extract_to_minio >> extract_to_postgres >> transform_to_staging
```

**Task 1 — generate_data**
Generates 100 synthetic transaction records per batch using Faker. Every record is stamped with a shared `batch_id` (format: `BATCH_YYYYMMDD_HHMMSS`) that links it to all downstream records in the same run. Records are inserted into MongoDB Atlas `mandera_db.transactions`.

**Task 2 — extract_to_minio**
Reads the current batch from MongoDB and uploads it as a JSON file to MinIO object storage under a date-partitioned path:
```
mandera-raw/year=2026/month=06/day=16/BATCH_20260616_131306.json
```
This creates a permanent, queryable archive of every raw batch — data that is never overwritten or deleted.

**Task 3 — extract_to_postgres**
Loads the same batch into the `raw_transactions` PostgreSQL landing table. After insertion, a row is written to `batch_log` recording the `batch_id`, `row_count`, `expected_count`, `variance` and `status`. A variance of zero confirms all records arrived intact.

**Task 4 — transform_to_staging**
Reads from `raw_transactions` and applies seven transformations to every record before inserting into `stg_transactions`:

| Field | Transformation |
|---|---|
| `total_amount` | Calculated as `quantity × unit_price` |
| `transaction_date` | Extracted from full timestamp to date only |
| `payment_method` | Lowercased |
| `status` | Lowercased |
| `region` | Title cased |
| `customer_name` | Title cased |
| `customer_email` | Lowercased |

After successful transformation, `batch_log` status is updated to `transformed` and `raw_transactions` is truncated — clearing the landing zone for the next batch.

---

## Database Schema

### raw_transactions
Landing zone for records extracted from MongoDB. Truncated after each successful transformation.

### stg_transactions
Clean, typed and transformed records ready for analytics queries.

### batch_log
Audit and monitoring table tracking every batch run.

| Column | Type | Description |
|---|---|---|
| batch_id | VARCHAR | Unique batch identifier |
| source | VARCHAR | Source system — mongodb |
| row_count | INTEGER | Records actually loaded |
| expected_count | INTEGER | Records expected per config |
| variance | INTEGER | Difference — 0 is healthy |
| status | VARCHAR | loaded / transformed / variance_detected |
| loaded_at | TIMESTAMP | When batch was logged |

---

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.10+
- MongoDB Atlas account (free tier)

### 1. Clone the repository
```bash
git clone https://github.com/akinzsoft/mandera-analytics.git
cd mandera-analytics
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install faker pymongo boto3 psycopg2-binary python-dotenv pandas
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your MongoDB Atlas connection string and credentials
```

### 4. Start services
```bash
docker compose up -d
```

This starts PostgreSQL (port 5432), MinIO (port 9000/9001) and Airflow (port 8080).

### 5. Create database tables
```bash
docker exec -i mandera_postgres psql -U mandera -d mandera_db < sql/create_raw_tables.sql
```

### 6. Run the pipeline manually
```bash
# Generate data
python3 generator/data_generator.py

# Extract to MinIO
python3 extraction/extract_mongo_to_minio.py

# Extract to PostgreSQL
python3 extraction/extract_mongo_to_postgres.py

# Transform to staging
python3 transformation/transform_orders.py
```

### 7. Run via Airflow
Open `http://localhost:8080`, log in with `admin / admin123`, enable the `mandera_batch_pipeline` DAG and click **Trigger DAG**.

---

## Environment Variables

```env
# MongoDB Atlas
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/
MONGO_DB=mandera_db
MONGO_COLLECTION=transactions

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mandera_db
POSTGRES_USER=mandera
POSTGRES_PASSWORD=your_password

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=your_secret
MINIO_BUCKET=mandera-raw

# Pipeline
BATCH_SIZE=100
```

---

## Pipeline Metrics

| Metric | Value |
|---|---|
| Records per batch | 100 |
| Average run duration | ~13 seconds |
| Airflow tasks | 4 |
| PostgreSQL tables | 3 |
| MinIO storage format | Partitioned JSON |
| Batch variance | 0 |

---

## Author

**Ayotunde Daniel Akinwumi**
Senior Software Engineer and Data Engineering Consultant

- GitHub: [github.com/akinzsoft](https://github.com/akinzsoft)
- LinkedIn: [linkedin.com/in/ayotundeakinwumi](https://linkedin.com/in/ayotundeakinwumi)
- Email: akinzsofts@gmail.com

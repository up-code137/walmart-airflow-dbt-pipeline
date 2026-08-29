# Walmart Retail Data Pipeline
## End-to-End Data Engineering Project | Airflow + dbt + Databricks + AWS S3 + Ghost DB

---

## Project Overview

A production-grade, end-to-end data engineering pipeline built for a retail domain (Walmart inspired dataset). This project simulates a real-world data platform where raw transactional data flows through an automated pipeline — from an agentic database and cloud storage all the way to business-ready Gold tables — orchestrated entirely by Apache Airflow.

**Key highlights:**
- Agentic Database (Ghost DB) with AI-powered SQL chatbot for schema creation
- CDC (Change Data Capture) for real-time incremental data ingestion
- Medallion Architecture — Bronze → Silver → Gold
- Metadata-driven pipeline — no static SQL queries
- SCD Type 2 (Slowly Changing Dimensions) for historical tracking
- Star Schema dimensional modeling
- Full orchestration via Apache Airflow running in Docker
- dbt for all transformations with built-in data quality tests

---

## Architecture

```
Ghost DB (Agentic PostgreSQL)          AWS S3
        |                                 |
        |-------- CDC -------+-----------|
                             |
                    Databricks (Bronze Layer)
                    Incremental Delta Lake Load
                             |
                    dbt Transformations
                             |
              +--------------+--------------+
              |                             |
        Silver Technical            Silver Business
        (Data Cleaning)             (One Big Table)
              |                             |
              +--------------+--------------+
                             |
                      Gold Layer
              +--------------+--------------+
              |              |              |
        Ephemeral       Dimensions      Fact Tables
        Models          (SCD Type 2)    (Star Schema)
                             |
                      Apache Airflow
                      (Orchestration)
                      10 Tasks | Docker
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Ghost DB | Agentic PostgreSQL database with AI SQL chatbot |
| AWS S3 | Raw data lake storage |
| Databricks | Data processing and Delta Lake |
| Apache Airflow | Pipeline orchestration |
| dbt | Data transformation (T in ELT) |
| Docker | Airflow deployment and containerization |
| Delta Lake | Incremental loading and ACID transactions |
| Python | DAG writing and data loading |
| SQL + Jinja | dbt model queries |
| uv | Modern Python package manager |

---

## Project Structure

```
walmart-retail-data-pipeline/
│
├── walmart_project/          # Source data and Ghost DB setup
│   ├── data/                 # Raw CSV files (customers, orders, products etc.)
│   ├── ddl/                  # DDL scripts for schema creation
│   └── load.py               # Script to load CSV data into Ghost DB
│
├── airflow/                  # Airflow + Docker setup
│   ├── dags/
│   │   └── orchestrate.py    # Main DAG with 10 tasks
│   ├── walmart_project/      # dbt project mounted in Airflow
│   │   ├── models/
│   │   │   ├── silver_t/     # Silver Technical layer
│   │   │   ├── silver_b/     # Silver Business layer (One Big Table)
│   │   │   └── gold/
│   │   │       ├── ephemeral/    # Ephemeral models (CTEs)
│   │   │       ├── dimensions/   # SCD Type 2 snapshots
│   │   │       └── fact/         # Final fact tables
│   │   ├── snapshots/        # dbt snapshots for SCD Type 2
│   │   ├── macros/           # Custom macros for dynamic schema
│   │   └── dbt_project.yml
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── requirements.txt
```

---

## Pipeline Walkthrough

### Step 1 — Source Setup
- Set up **Ghost DB** — an agentic PostgreSQL database with built-in AI SQL chatbot
- Used Ghost AI agent to create schemas and tables by writing natural language prompts
- Loaded 6 raw CSV tables into `raw` schema: customers, stores, products, employees, orders, order_items

### Step 2 — Bronze Layer (Incremental Ingestion)
- Connected Ghost DB to Databricks via **CDC (Change Data Capture)**
- CDC automatically detects new and changed records — no full reload needed
- Also connected **AWS S3** as secondary data source
- Data lands in Databricks **Delta Lake** as Bronze tables

### Step 3 — Silver Technical Layer (dbt)
- Used **dbt incremental models** to transform raw Bronze data
- Applied data cleaning, type casting, timestamp conversion
- Built-in **data quality tests** — not_null, unique checks
- Tables: customers_t, orders_t, products_t, employees_t, stores_t, order_items_t

### Step 4 — Silver Business Layer — One Big Table (dbt)
- Built a **metadata-driven pipeline** — no static SQL queries
- Used Jinja templating with a config-driven approach
- Joined all Silver Technical tables into one wide **One Big Table (OBT)**
- This eliminates complex joins at query time for analysts

### Step 5 — Gold Layer (dbt)
- **Ephemeral models** — temporary CTEs used as building blocks, not stored as tables
- **Snapshots (SCD Type 2)** — tracks historical changes with valid_from, valid_to, is_current columns
- **Fact tables** — final business-ready tables for reporting
- **Star Schema** — dimension tables surrounding central fact table

### Step 6 — Orchestration (Apache Airflow + Docker)
- Deployed Airflow using **Docker Compose**
- Wrote `orchestrate.py` DAG with **10 sequential tasks**
- DAG flow:

```
ingest_cdc → clean_target → source_freshness → 
silver_technical → silver_technical_tests → 
silver_business → silver_business_tests → 
gold_ephemeral → gold_dimensions → gold_facts
```

- If any task fails — pipeline stops immediately
- Full logs available for every task in Airflow UI

---

## Key Concepts Implemented

| Concept | Implementation |
|---|---|
| CDC | Ghost DB → Databricks auto-detect changes |
| Incremental Loading | dbt incremental models with unique_key |
| SCD Type 2 | dbt snapshots with valid_to/valid_from |
| Metadata Driven Pipeline | Jinja config-driven One Big Table |
| Star Schema | Gold fact + dimension tables |
| Data Quality Tests | dbt not_null, unique, relationship tests |
| Ephemeral Models | CTEs in Gold layer — no physical table |
| Orchestration | 10-task Airflow DAG in Docker |

---

## What I Learned

- How to set up and use an **Agentic Database** (Ghost DB) with AI-powered schema creation
- End-to-end pipeline design from raw source to business-ready Gold tables
- **CDC** — detecting and processing only changed records for efficiency
- **dbt** as the transformation layer — models, tests, snapshots, macros, Jinja templating
- **Apache Airflow** — DAG creation, task dependencies, debugging, Docker deployment
- **Metadata-driven pipelines** — building dynamic SQL using configuration instead of hardcoded queries
- **SCD Type 2** — maintaining full historical data with slowly changing dimensions
- Difference between **Star Schema** and **One Big Table** approaches

---

## Setup Instructions

### Prerequisites
- Docker Desktop installed
- Databricks workspace (free edition works)
- AWS S3 bucket
- Ghost DB account (ghost.build)
- Python 3.10+

### 1. Clone the repository
```bash
git clone https://github.com/up-code137/walmart-retail-data-pipeline.git
cd walmart-retail-data-pipeline
```

### 2. Set up Ghost DB
```bash
pip install ghost-sdk
ghost create --name walmart_db
ghost api-key create --name walmart_api
```

### 3. Load data into Ghost DB
```bash
cd walmart_project
pip install psycopg2-binary
export WALMART_DATABASE_URL="your_ghost_connection_string"
python load.py
```

### 4. Set up environment variables
```bash
cd airflow
cp .env.example .env
# Fill in your Databricks host, token, and Fernet key
```

### 5. Start Airflow
```bash
docker compose build
docker compose up -d
```

### 6. Access Airflow UI
- Open `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`
- Trigger the `orchestrate` DAG

---

## Author

**Sameer Sinha**
- GitHub: [up-code137](https://github.com/up-code137)
- LinkedIn: [sameersinha-de](https://linkedin.com/in/sameersinha-de)
- Email: sameersinha137@gmail.com
- Startup: [PhonePlus+](https://phoneplus.in)

---

*This project is built for learning purposes using a Walmart-inspired retail dataset.*

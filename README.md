# FinPulse: Real-Time Banking Transaction Ingestion & Financial Risk Observability Platform

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Kafka/Redpanda](https://img.shields.io/badge/Streaming-Redpanda%20%2F%20Kafka-red.svg)](https://redpanda.com/)
[![DuckDB](https://img.shields.io/badge/OLAP-DuckDB-yellow.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An end-to-end event-driven financial data platform designed to ingest high-throughput banking card transactions, enforce automated **Data Quality & Quarantine Gates (DLQ)**, persist dimensional analytical models into a high-performance **OLAP Data Warehouse**, and deliver real-time **Financial Risk Observability** via an interactive operational dashboard.

---

## 🏛️ System Architecture

```
[ Historical IBM Card Data ]
             │
             ▼
┌───────────────────────────────┐
│   Event Streaming Replayer    │ ──► [ Simulates Real-time Swipes + Fault Injection ]
└───────────────────────────────┘
             │
             ▼ (JSON Event Stream)
┌───────────────────────────────┐
│   Redpanda / Kafka Cluster    │ ──► Topic: banking.transactions.raw
└───────────────────────────────┘
             │
             ▼ (Stream Consumer & Validation Engine)
┌─────────────────────────────────────────────────────────────────┐
│                   Data Quality & Quarantine Gate                │
│   • Completeness (Null checks on User, Card, Amount)            │
│   • Validity (Amount > 0, Future Timestamps, MCC validation)     │
│   • Logic Consistency (Amount <= Card Credit Limit)             │
└─────────────────────────────────────────────────────────────────┘
         │                                       │
   (Valid Stream)                          (Invalid / Corrupted)
         │                                       │
         ▼                                       ▼
┌─────────────────────────────┐         ┌─────────────────────────────┐
│   OLAP Data Warehouse       │         │   Dead Letter Queue (DLQ)   │
│   (Star Schema: Fact & Dims)│         │   & Quarantine Audit Log    │
└─────────────────────────────┘         └─────────────────────────────┘
         │                                       │
         └───────────────────┬───────────────────┘
                             │
                             ▼
             ┌───────────────────────────────┐
             │  Live Streamlit Ops Center    │
             │  • Real-time Throughput Ticker│
             │  • Data Health & Error Matrix │
             │  • Credit Utilization Alerts  │
             └───────────────────────────────┘
```

---

## 📂 Repository Structure

```
├── data/
│   └── dataset_1/                  # Raw IBM Credit Card Transactions dataset
├── docker/
│   └── docker-compose.yml          # Containerized Redpanda, Postgres, Adminer
├── docs/                           # Project requirements, Star Schema ERD & Defense documents
├── src/
│   ├── config/                     # Environment settings and connection management
│   ├── producer/                   # Historical event replayer & Fault injection service
│   ├── pipeline/                   # Real-time consumer, Data Quality Gate & DWH loader
│   ├── analytics/                  # Financial risk scoring (Utilization spike, Velocity)
│   └── dashboard/                  # Live interactive Streamlit operations center
├── tests/                          # Automated unit and integration test suites
├── .env.example                    # Environment configuration template
├── .gitignore                      # Git exclusion rules (Large CSV protection)
├── README.md                       # Project documentation
└── requirements.txt                # Python project dependencies
```

---

## ⚡ Quickstart Guide

### 1. Prerequisites
- Python 3.10+
- Docker & Docker Compose (optional for local Redpanda streaming cluster)

### 2. Environment Setup
```bash
# Clone the repository
git clone https://github.com/KANH12/realtime-banking-transaction-pipeline.git
cd realtime-banking-transaction-pipeline

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the Platform
```bash
# Start Streamlit Operations Dashboard
streamlit run src/dashboard/app.py
```

---

## 🛡️ Data Quality & Risk Rules

| Rule Category | Check Name | Target Fields | Action on Failure |
| :--- | :--- | :--- | :--- |
| **Completeness** | `NOT_NULL_CRITICAL` | `User`, `Card`, `Amount`, `Time` | Route to Quarantine Table with `ERR_NULL` |
| **Validity** | `POSITIVE_AMOUNT` | `Amount > 0` | Flag `ERR_NEGATIVE_VALUE` & isolate |
| **Integrity** | `CARD_EXISTENCE` | `Card` exists in `Dim_Cards` | Reject unmapped orphan transaction |
| **Business Logic** | `LIMIT_BREACH` | `Amount <= Credit_Limit` | Raise `ALERT_OVERLIMIT` and flag risk |
| **Financial Risk** | `HIGH_UTILIZATION` | `Current_Balance / Limit > 0.85` | Trigger Critical Watchlist Alert on Dashboard |

---

## 👨‍💻 Author
- **Le Nguyen Bao Khang** (Data Engineer Intern / Fresher)
- GitHub: [@KANH12](https://github.com/KANH12)
- LinkedIn: [in/khang-le-310131329](https://linkedin.com/in/khang-le-310131329)

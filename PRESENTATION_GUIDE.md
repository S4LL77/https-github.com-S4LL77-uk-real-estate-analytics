# 🎯 Recruiter & Tech Lead Guide: UK Real Estate Analytics

Welcome to the technical showcase of this End-to-End Data Engineering project. This repository demonstrates a complete Medallion Architecture (Bronze → Silver → Gold) using a modern data stack.

---

## 🏗️ 1. How to Visualize the Architecture (Data Lineage)
To see the "brain" of the project and how data flows from Government CSVs to clean analytical tables:
1.  **Run**: `docker run --rm -it -p 8080:8080 -v ${PWD}:/usr/app -w /usr/app/dbt_project ghcr.io/dbt-labs/dbt-snowflake:1.7.latest docs serve --port 8080`
2.  **View**: Open [http://localhost:8080](http://localhost:8080)
3.  **Showcase**: Click the **Blue Icon** (bottom right) to see the **Lineage Graph**. This proves mastery of `dbt`, `Snowflake`, and `SCD Type 2` history tracking.

## 📊 2. How to View the Insights (Streamlit Dashboard)
The visualization layer for non-technical stakeholders:
1.  **Run**: `python -m streamlit run dashboard.py`
2.  **Highlights**: 
    - **Live Connection**: Directly querying Snowflake Gold Marts.
    - **KPIs**: Median Price calculation (£565k for London matches OS data).
    - **Trends**: 2024 price volatility analysis using Plotly.

## 🧠 3. How to Test the Intelligence (FastAPI)
The serving layer for machine learning / downstream apps:
1.  **Run**: `python -m uvicorn api.main:app --reload`
2.  **Interact**: Open [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
3.  **Endpoint to Show**: `/predict/estimate`. Input a postcode (e.g. `SW1A`) and watch the causal model adjust prices based on real-time Bank of England interest rates.

---

## 🛠️ Technical Talking Points for the Interview

### **Data Engineering Excellence**
- **Medallion Architecture**: Implemented 3 distinct layers to separate raw ingestion from business-ready data.
- **Infrastructure as Code (IaC)**: Used **Terraform** to manage Snowflake warehouses and RBAC roles (see `terraform/` folder).
- **Data Quality**: 29 unit tests (`pytest`) + dbt tests to catch duplicates and nulls at the source.
- **CI/CD**: Fully functional GitHub Actions pipelines for automated linting and dbt compilation.

### **Strategic Decisions**
- **SCD Type 2**: Why? To track how property values and features change over time without losing history.
- **Parquet Storage**: Used for the Bronze layer to optimize for Snowflake's columnar ingestion performance.
- **Secure Views**: Implemented for GDPR-compliant data masking of street-level addresses.

---

## 📁 Repository Map
- `/ingestion`: Python extraction scripts (Land Registry, BoE, ONS).
- `/dbt_project`: The transformation logic (SQL models & tests).
- `/api`: FastAPI serving layer.
- `/orchestration`: Airflow DAGs for automated scheduling.
- `/terraform`: Infrastructure definitions.

---
**Author**: [Your Name/GitHub Profile]
**Focus**: Data Engineering | Analytics Engineering | Cloud Data Warehousing

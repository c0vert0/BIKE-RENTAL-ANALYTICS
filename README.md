# 🚀 Bike-Rentals Data Pipeline (Snowflake + AWS + Power BI)

## 📌 Overview

This project builds an end-to-end data pipeline for processing insurance policy and claims data using Snowflake and AWS. The pipeline ingests raw CSV/JSON data, cleans and validates it, performs transformations, and generates analytical datasets for reporting.

---

## 🧱 Architecture

```
S3 (Raw Files)
   ↓
Snowflake Stage
   ↓
Snowpipe (Auto Ingestion)
   ↓
RAW Layer (No transformation)
   ↓
VALIDATED Layer (Cleaning & standardization)
   ↓
CURATED Layer (Business metrics)
   ↓
Insights Dashboard
```

---

## 🛠️ Tech Stack

* Snowflake (Data Warehouse)
* AWS S3 (Storage)
* Snowpipe (Ingestion)
* SQL (Transformation)
* Snowflake BI (Visualization)
* GitHub (Version Control)

---

## 📂 Data Source

* bikes_master.csv
* rentals_master.csv
* stations_master.csv
* users_master.csv

---

## ⚙️ Key Features

### 1. Automated Data Ingestion

* Used Snowpipe for continuous loading
* Handled both CSV and JSON formats

### 2. Data Cleaning & Validation

* Removed invalid records using TRY_TO_NUMBER / TRY_TO_DATE
* Standardized categorical fields (e.g., status)
* Identified null and inconsistent values

### 3. Incremental Load Handling

* Used MERGE logic to avoid duplicates
* Processed incremental files efficiently

### 4. Data Modeling

* RAW → VALIDATED → CURATED layered architecture
* Ensured separation of concerns

### 5. Business Insights

* Customer-level premium analysis
* Claims vs policies correlation
* Data quality reporting

---

## 📁 Repository Structure

```
/data
/sql
   ├── raw_tables.sql
   ├── validated_layer.sql
   ├── curated_layer.sql
/dashboard
/docs
README.md
```

---

## 👨‍💻 My Contribution

* Built data cleaning logic using Snowflake SQL
* Designed layered data architecture
* Implemented transformation queries
* Contributed to data validation and reporting

---

## 🚀 Future Improvements

* Add real-time streaming ingestion
* Implement data quality monitoring dashboards
* Optimize query performance

---

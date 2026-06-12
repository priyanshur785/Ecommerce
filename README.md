# E-Commerce Analytics Data Pipeline

## Overview

This project demonstrates the implementation of an end-to-end Data Engineering pipeline for an E-Commerce platform using Databricks, PySpark, Delta Lake, and Spark SQL.

The solution follows the **Medallion Architecture (Bronze, Silver, Gold)** to transform raw transactional data into analytics-ready datasets for reporting, business intelligence, and decision-making.

The pipeline processes customer, product, category, brand, date, and order transaction data while ensuring data quality, consistency, and scalability.

---

## Project Objectives

- Build a scalable Data Engineering pipeline.
- Implement ETL workflows using PySpark.
- Apply Medallion Architecture principles.
- Create analytics-ready Fact and Dimension tables.
- Generate business KPIs for E-Commerce reporting.
- Gain hands-on experience with Databricks and Delta Lake.

---

## Technology Stack

### Data Engineering
- Databricks
- PySpark
- Delta Lake
- Spark SQL
- ETL Pipelines

### Programming
- Python

### Data Storage
- Delta Tables

### Architecture
- Medallion Architecture
  - Bronze Layer
  - Silver Layer
  - Gold Layer

---

## Dataset Structure

The project uses multiple E-Commerce datasets.

### Dimension Data

- Customers
- Products
- Categories
- Brands
- Date

### Fact Data

- Order Items
- Sales Transactions

---

## Repository Structure

```text
Ecommerce/
│
├── Source Data/
│   ├── brands/
│   ├── category/
│   ├── customers/
│   ├── date/
│   ├── products/
│   └── order_items/
│
├── project_Ecommerce/
│   │
│   ├── 1_setup/
│   │
│   ├── med_procc_dim/
│   │   ├── 1_dim_bronze
│   │   ├── 2_dim_silver
│   │   └── 3_dim_gold
│   │
│   └── 3_fact_processing/
│       ├── 1_fact_bronze
│       ├── 2_fact_silver
│       └── 1_fact_gold
│
└── README.md
```

---

## Architecture

### Bronze Layer

**Purpose**
- Raw data ingestion
- Preserve source data
- Minimal transformations

**Activities**
- Load source files
- Store raw records
- Create Bronze Delta Tables

---

### Silver Layer

**Purpose**
- Data cleansing
- Validation
- Standardization

**Activities**
- Remove duplicates
- Handle missing values
- Data type corrections
- Business rule validation

---

### Gold Layer

**Purpose**
- Business-ready datasets
- Analytics and reporting

**Activities**
- Fact table creation
- Dimension table creation
- KPI generation
- Aggregated reporting datasets

---

## Dimension Processing

The Dimension Processing pipeline creates and maintains business entities used for analytical reporting.

### Dimensions Created

- Customer Dimension
- Product Dimension
- Brand Dimension
- Category Dimension
- Date Dimension

These dimensions provide descriptive context for transactional data and support business intelligence reporting.

---

## Fact Processing

The Fact Processing pipeline processes transactional and sales-related records.

### Features

- Sales aggregation
- Revenue calculations
- Transaction processing
- KPI generation

Fact tables are linked with dimension tables to support analytical queries and reporting.

---

## ETL Workflow

1. Ingest raw E-Commerce datasets.
2. Load records into Bronze Delta tables.
3. Clean and standardize data in Silver tables.
4. Create Fact and Dimension models in Gold tables.
5. Generate business metrics and reporting datasets.

---

## Business KPIs Generated

- Gross Sales
- Net Sales
- Revenue Analysis
- Discount Analysis
- Coupon Utilization
- Product Performance Analysis
- Customer Purchase Insights

---

## Key Learning Outcomes

Through this project, I gained practical experience in:

- Data Engineering Fundamentals
- Databricks Workspace Management
- PySpark Transformations
- Spark SQL
- ETL Pipeline Design
- Delta Lake
- Data Warehousing Concepts
- Medallion Architecture
- Fact and Dimension Modeling
- Business Intelligence Data Preparation

---

## Future Enhancements

- Workflow orchestration using Apache Airflow
- Automated data quality monitoring
- Power BI dashboard integration
- Cloud deployment on Azure/AWS
- Real-time streaming data ingestion

---

## Author

**Priyanshu Rai**

B.Tech – Computer Science & Engineering (Data Science)

Data Engineering Intern @ Capgemini

- GitHub: https://github.com/priyanshur785
- LinkedIn: https://linkedin.com/in/priyanshuu780

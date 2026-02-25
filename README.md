In a high-pressure hospital environment, delayed data means delayed treatment. This project implements an end-to-end data pipeline to analyze patient flow, triage efficiency, and treatment outcomes. By moving from flat file storage to a Cloud Data Warehouse (Snowflake) with Apache Spark processing, we enable hospital administrators to identify bottlenecks in real-time.

# Core Objectives
Wait-Time Analysis: Calculate metrics to reduce the gap between patient arrival and treatment.
Resource Optimization: Identify peak emergency hours and most common medical conditions.
Security & Compliance: Ensure patient privacy through Role-Based Access Control (RBAC) and Dynamic Data Masking.

# System Architecture
The project follows a modular 4-tier architecture:
Ingestion Layer: Raw patient logs (CSV) and medical vitals (JSON).
Processing Layer: Apache Spark (Batch & Streaming) for cleaning and feature engineering.
Warehouse Layer: Snowflake utilizing a Star Schema for high-performance analytics.
Security Layer: Implementing RBAC, Masking Policies, and Governance.

# Data Modeling
The data is structured into a Star Schema to optimize query performance for BI tools like Tableau and Power BI.
Fact Table: fact_emergency_visits (Metrics: wait_time, treatment_time).

Dimensions:

dim_patient: Patient demographics.
dim_hospital: Facility locations and metadata.
dim_triage: Severity levels (Critical, High, Medium, Low).
dim_condition: Injury/Disease classification.
dim_time: Granular date/time intelligence.

# Key Features
1. Advanced ETL/ELT Pipelines
Python: Used for initial data extraction and logical mapping.
PySpark: Handles distributed processing for large datasets.
Streaming: Spark Structured Streaming ready for real-time triage alerts.

2. Snowflake Optimization
Clustering: Optimized storage for fast filtering on hospital_id and arrival_time.
Semi-Structured Data: Native support for JSON vitals using the VARIANT data type.
Time Travel: Enabled for data recovery and historical auditing (up to 90 days).

3. Enterprise Security & Governance
RBAC: Segregated roles for Admins, Doctors, and Analysts.
Dynamic Data Masking: Automatically hides patient_id and sensitive conditions from non-authorized roles.
Audit Logging: Full query history tracking via Snowflake's ACCESS_HISTORY.

# Tech Stack
Language: Python (Pandas, PySpark)
Processing: Apache Spark (Batch + Streaming)
Data Warehouse: Snowflake

Database Design: Star Schema Modeling

SQL: CTEs, Window Functions, RBAC, Masking Policies

Visualization: Power BI / Tableau

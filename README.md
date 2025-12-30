# Azure End-to-End Data Warehouse Project

## About the Project

This repository contains an **end-to-end data engineering project built on Microsoft Azure**.
I created this project to understand how data moves from a raw source to a fully analytics-ready data warehouse, using tools that are commonly used in real production environments.

The project follows a **modern Medallion Architecture (Bronze, Silver, Gold)** and uses the **Adventure Works** dataset, which is well-suited for practicing transformations, joins, and analytical queries.

---

## What This Project Does

At a high level, the project:

* Ingests raw data from an API
* Stores it in Azure Data Lake
* Cleans and transforms the data using Spark
* Serves the final data through a data warehouse
* Connects the warehouse to Power BI for reporting

The idea was not just to use individual tools, but to understand **how everything fits together end-to-end**.

---

## Architecture Overview

The data flows through the following layers:

```
Source (API)
   ↓
Bronze Layer – Raw data stored in ADLS Gen2
   ↓
Silver Layer – Cleaned & transformed data using Databricks (Spark)
   ↓
Gold Layer – Analytics layer in Azure Synapse
   ↓
Power BI – Reporting and dashboards
```

Each layer has a clear responsibility, which helps keep the pipeline clean, scalable, and easier to maintain.

---

## Azure Services Used

* **Azure Data Factory** – for data ingestion and orchestration
* **Azure Data Lake Storage Gen2** – for storing raw and processed data
* **Azure Databricks** – for data transformation using PySpark
* **Azure Synapse Analytics** – for building the data warehouse
* **Power BI** – for visualization and analysis

---

## Dataset

* **Adventure Works**
* Chosen because it contains multiple related tables and realistic business data
* Works well for practicing joins, transformations, and warehouse-style queries

---

## Data Pipeline Breakdown

### 1. Data Ingestion (Bronze Layer)

* Used Azure Data Factory to pull data from an HTTP/API source
* Stored the data in Azure Data Lake in its original, raw format
* No transformations at this stage — just ingestion

### 2. Data Transformation (Silver Layer)

* Used Azure Databricks with Apache Spark
* Cleaned the data and applied transformations
* Created refined datasets suitable for analytics
* Saved the transformed data back to the Data Lake

### 3. Data Warehouse (Gold Layer)

* Used Azure Synapse Analytics as the serving layer
* Queried data using `OPENROWSET()` and external tables
* Designed the data to support analytical and reporting use cases

### 4. Reporting

* Connected Azure Synapse to Power BI
* Built dashboards to show how the data can be consumed by business users

---

## What I Learned

* How an **end-to-end data pipeline** works in the cloud
* How different Azure services integrate with each other
* Practical experience with **ADF, Databricks, Spark, and Synapse**
* How data engineers support analytics and BI teams
* Why architecture and layering matter in real projects

---

## Notes

* This project was completed by following a guided tutorial for learning purposes
* All steps were implemented hands-on to build real understanding
* Possible next steps:

  * Incremental loads
  * Data validation and quality checks
  * Pipeline monitoring and alerting
  * Performance and cost optimizations

---

## Why This Project

I built this project to move beyond theory and actually work with tools used in real data engineering roles.
It reflects my learning journey and my growing comfort with designing and explaining data pipelines from start to finish.


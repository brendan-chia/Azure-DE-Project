# Special Topic in Data Engineering - Azure Data Pipeline Project

This repository contains my project for the subject **Special Topic in Data Engineering**.  
It demonstrates an end-to-end Medallion-style pipeline using Azure Data Factory/Synapse orchestration, ADLS Gen2 storage layers, and Databricks transformations.

This repo is also maintained as a documentation-first record of the implementation so I can preserve project evidence without repeatedly running cloud resources and incurring charges on my Microsoft Azure student account.

## Project Context

This implementation is designed for a **learning and portfolio scenario**, not for production.
Because of that, I optimized for:

- fast iteration and experimentation
- clear, centralized orchestration logic
- low-cost, serverless-first execution

## Script Activity instead of Stored Procedure

I intentionally used **Script Activity** instead of a stored procedure because this is a non-production academic project:

- **No database pre-deployment**: the pipeline can run without manually creating procedures first.
- **Single place for orchestration logic**: dynamic SQL is visible and editable in the pipeline flow.
- **Faster iteration**: changing view-generation logic is immediate during testing.
- **Serverless-friendly approach**: aligned with an on-demand analytics pattern for portfolio work.

In short, Script Activity keeps the project agile and easier to maintain while I focus on pipeline design and data engineering concepts.

## Architecture Summary

- **Bronze layer**: raw parquet data in ADLS Gen2.
- **Silver layer**: cleaned Delta tables.
- **Gold layer**: standardized/analytics-ready Delta tables exposed through serverless SQL views.
- **Orchestration**: pipeline loop dynamically generates SQL per table.

### Pipeline SQL/Loop Logic

- `ForEach Loop/dynamic_sql_query.md`  
	Dynamic query expression used in looped extraction (`SELECT * FROM schema.table`).
- `Synapse Analytics/script_activity.sql`  
	SQL template used by Script Activity to create or alter views dynamically from Gold Delta paths.
- `Synapse Analytics/script_activity.md`  
	Notes explaining why `OPENROWSET` with `FORMAT = 'DELTA'` is used in a serverless model.

### Environment Notes

- `Resource Group/resource-group.jpg`  
	Screenshot artifact for Azure resource setup used in the project.

## Databricks Code Explanation

### 1) Bronze to Silver Notebook

File: `databricks-notebook/bronze-to-silver.py`

Main flow:

1. Configure Spark access to ADLS Gen2.
2. Read bronze parquet tables from `SalesLT`.
3. Detect columns containing `Date`/`date` and normalize them to `yyyy-MM-dd`.
4. Write transformed data as **Delta** format into the Silver container (`mode('overwrite')`).

Purpose:

- move from raw zone to cleaned/typed data zone
- enforce consistent date formatting early
- prepare data for downstream modeling

### 2) Silver to Gold Notebook

File: `databricks-notebook/silver-to-gold.py`

Main flow:

1. Configure storage access and mount Bronze/Silver/Gold containers.
2. Define helper function to convert column names from camelCase/PascalCase to `snake_case`.
3. Iterate through Silver Delta tables.
4. Apply column renaming standardization.
5. Write standardized Delta output to Gold (`mode('overwrite')`).

Purpose:

- enforce naming conventions for analytics consumption
- provide cleaner datasets for SQL views and BI tools
- keep Gold layer consistent across all tables

## End-to-End Flow

1. Raw parquet lands in Bronze.
2. Databricks notebook transforms Bronze to Silver (data cleaning/date normalization).
3. Databricks notebook transforms Silver to Gold (schema/naming standardization).
4. Pipeline ForEach + Script Activity creates/updates serverless SQL views over Gold Delta paths.
5. Reporting tools can query curated views without duplicating storage.

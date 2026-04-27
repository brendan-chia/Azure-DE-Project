# Databricks notebook source
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # In Databricks, env vars can be injected via cluster config/secret scopes.
    pass

# 1. Define your credentials
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "aztutorial")
storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

if not storage_account_key:
    raise ValueError("Missing AZURE_STORAGE_ACCOUNT_KEY environment variable")

# 2. Configure Spark to talk to your Data Lake directly
# This stays active for as long as this notebook (or any notebook using this cluster) is running
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("✅ Spark session configured for direct access to aztutorial.")

# 3. Create your path variables (this makes the rest of the tutorial easy to follow)
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
gold_path   = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"

print(f"Paths ready:\n - Bronze: {bronze_path}\n - Silver: {silver_path}\n - Gold: {gold_path}")

# COMMAND ----------

df = spark.read.format('parquet').load(f'abfss://bronze@{storage_account_name}.dfs.core.windows.net/SalesLT/Address/')

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df['ModifiedDate'].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Date Transformation for tables
# MAGIC

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls(f'abfss://bronze@{storage_account_name}.dfs.core.windows.net/SalesLT/'):
    table_name.append(i.name.split('.')[0])

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in table_name:
    path = f'abfss://bronze@{storage_account_name}.dfs.core.windows.net/SalesLT/' + i + '/'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

    output_path = f'abfss://silver@{storage_account_name}.dfs.core.windows.net/SalesLT/' + i + '/'
    df.write.format('delta').mode('overwrite').save(output_path)

    

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC This code is a Bronze → Silver data pipeline in Azure Databricks. Here's what it does:
# MAGIC
# MAGIC Overall purpose: Reads raw parquet files from Azure Data Lake (bronze layer), cleans them, and saves them as Delta tables in the silver layer.
# MAGIC
# MAGIC Step by step:
# MAGIC
# MAGIC Loops through all tables in table_name (e.g. Address, Customer, Product, etc. from the SalesLT schema)
# MAGIC Reads each table as a parquet file from the bronze container in Azure Data Lake Storage (ADLS)
# MAGIC Fixes date columns — for any column with "Date" or "date" in its name, it converts the raw UTC timestamp into a clean yyyy-MM-dd string format
# MAGIC Writes to silver layer — saves the cleaned data as a Delta table (with overwrite mode) in the silver container
# MAGIC In the context of a Medallion Architecture:
# MAGIC
# MAGIC 🥉 Bronze = raw parquet files, as-is from the source
# MAGIC 🥈 Silver = cleaned/transformed Delta tables (this is what this code produces)
# MAGIC 🥇 Gold = aggregated/business-ready data (next step)
# MAGIC So essentially this is a data cleaning and format standardisation step for the SalesLT dataset before it's used for analytics or reporting.
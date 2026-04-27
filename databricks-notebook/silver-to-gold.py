# Databricks notebook source
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # In Databricks, env vars can be injected via cluster config/secret scopes.
    pass

storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "aztutorial")
storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

if not storage_account_key:
    raise ValueError("Missing AZURE_STORAGE_ACCOUNT_KEY environment variable")

configs = {
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net": storage_account_key
}

# Mount Bronze
try:
    dbutils.fs.mount(
      source = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/",
      mount_point = "/mnt/bronze",
      extra_configs = configs
    )
except Exception as e:
    print(f"Bronze mount failed: {e}")


# Mount Silver
try:
    dbutils.fs.mount(
      source = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/",
      mount_point = "/mnt/silver",
      extra_configs = configs
    )
except Exception as e:
    print(f"Silver mount failed: {e}")


# Mount Gold
try:
    dbutils.fs.mount(
      source = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/",
      mount_point = "/mnt/gold",
      extra_configs = configs
    )
except Exception as e:
    print(f"Gold mount failed: {e}")


print("Mounting script completed!")


# COMMAND ----------

# Set the account key
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

df = spark.read.format('parquet').load(f'abfss://bronze@{storage_account_name}.dfs.core.windows.net/SalesLT/Address/')

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

def rename_columns_to_snake_case(df):
    """
    Convert column names from PascalCase or camelCase to snake_case in a PySpark DataFrame.

    Args:
        df (DataFrame): The input DataFrame with columns to be renamed.

    Returns:
        DataFrame: A new DataFrame with column names converted to snake_case.
    """

    # Get the list of column names
    column_names = df.columns

    # Dictionary to hold old and new column name mappings
    rename_map = {}

    for old_col_name in column_names:
        # Convert column name from PascalCase or camelCase to snake_case
        new_col_name = "".join([
            "_" + char.lower() if (
                char.isupper()                          # Check if the current character is uppercase
                and idx > 0                             # Ensure it's not the first character
                and not old_col_name[idx - 1].isupper()  # Ensure the previous character is not uppercase
            ) else char.lower()  # Convert character to lowercase
            for idx, char in enumerate(old_col_name)
        ]).lstrip("_")  # Remove any leading underscore

        # Avoid renaming to an existing column name
        if new_col_name in rename_map.values():
            raise ValueError(f"Duplicate column name found after renaming: '{new_col_name}'")

        # Map the old column name to the new column name
        rename_map[old_col_name] = new_col_name

    # Rename columns using the mapping
    for old_col_name, new_col_name in rename_map.items():
        df = df.withColumnRenamed(old_col_name, new_col_name)

    return df

    # example usage , df = rename_column_to_snake_case(df)


# COMMAND ----------

df = rename_columns_to_snake_case(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC All table columns transformation (col names)

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls(f'abfss://silver@{storage_account_name}.dfs.core.windows.net/SalesLT/'):
    table_name.append(i)

table_name

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls(f'abfss://silver@{storage_account_name}.dfs.core.windows.net/SalesLT/'):
    table_name.append(i.name.split('/')[0])

table_name

# COMMAND ----------

for name in table_name:
    path = f'abfss://silver@{storage_account_name}.dfs.core.windows.net/SalesLT/' + name
    print(path)
    df = spark.read.format('delta').load(path)

    df = rename_columns_to_snake_case(df)

    output_path = f'abfss://gold@{storage_account_name}.dfs.core.windows.net/SalesLT/' + name + '/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

display(df)

# COMMAND ----------


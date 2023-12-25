# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

read_df = spark.read.parquet(f"{silver_folder_path}/races")

# COMMAND ----------

## SQL syntax
races_filtered_df =  read_df.filter("race_year = 2019")

# COMMAND ----------

## Python syntax
races_filtered_df =  read_df.filter(read_df.race_year == 2019)

# COMMAND ----------

## Multiple conditions SQL
races_filtered_df =  read_df.filter("race_year = 2019 AND round <= 5")

# COMMAND ----------

## Multiple conditions python
races_filtered_df =  read_df.filter((read_df.race_year == 2019) & (read_df.round <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------



# Databricks notebook source
# MAGIC %md 
# MAGIC ### Produce constructor standings

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_list = spark.read.parquet(f"{gold_folder_path}/race_results")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list,'race_year')

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year", "team").agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

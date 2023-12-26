# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agregate functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC #### Built-in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_folder_path}/race_results").filter("race_year = 2020")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### GroupBy Functions

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

race_results_df.select(count("*")).show()

# COMMAND ----------

race_results_df.select(countDistinct("race_name")).show()

# COMMAND ----------

race_results_df.select(sum("points")).show()

# COMMAND ----------

race_results_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

race_results_df.filter("driver_name = 'Lewis Hamilton'")\
               .select(sum("points"), countDistinct("race_name"))\
               .withColumnRenamed("sum(points)","total_points")\
               .withColumnRenamed("count(DISTINCT race_name)","number_of_races")\
               .show()

# COMMAND ----------

race_results_df.groupBy("driver_name")\
               .sum("points")\
               .show()

# COMMAND ----------

race_results_df.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))\
               .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

demo_window_df = spark.read.parquet(f"{gold_folder_path}/race_results").filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_window_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
display(demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)))

# COMMAND ----------



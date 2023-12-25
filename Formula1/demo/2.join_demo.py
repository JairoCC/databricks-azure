# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{silver_folder_path}/circuits").withColumnRenamed("name","circuit_name").filter("circuit_id < 70")
races_df = spark.read.parquet(f"{silver_folder_path}/races").withColumnRenamed("name","race_name").filter("race_year = 2019")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuite_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuite_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Outer Join

# COMMAND ----------

# Left outer join
race_circuite_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuite_left_df)

# COMMAND ----------

#Rigth outer join
race_circuite_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuite_right_df)

# COMMAND ----------

# full outer join
race_circuite_full_outer_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuite_full_outer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Joins

# COMMAND ----------

race_circuite_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuite_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti join

# COMMAND ----------

race_circuite_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuite_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Joins

# COMMAND ----------

race_circuite_df = races_df.crossJoin(circuits_df)

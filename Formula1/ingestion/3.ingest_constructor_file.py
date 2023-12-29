# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest contructor.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read \
    .schema(constructor_schema) \
    .json(f"{bronze_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwaanted columns from the dataframes

# COMMAND ----------

constructor_drop_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("ingestion_date",  current_timestamp())\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")

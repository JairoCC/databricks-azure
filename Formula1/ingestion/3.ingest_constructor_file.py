# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest contructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read \
    .schema(constructor_schema) \
    .json("/mnt/formula1dljc/bronze/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwaanted columns from the dataframes

# COMMAND ----------

constructor_drop_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("ingestion_date",  current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")

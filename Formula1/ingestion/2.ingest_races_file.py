# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Schema

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year",IntegerType(), True),
    StructField("round",IntegerType(), True),
    StructField("circuitId",IntegerType(), True),
    StructField("name",StringType(), True),
    StructField("date",DateType(), True),
    StructField("time",StringType(), True),
    StructField("url",StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{bronze_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Add Ingestion date and race_timestamp to dataframe and rename columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

races_rename_df = races_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("date"),col("time"))

# COMMAND ----------

races_with_timestamp_df = races_rename_df.withColumn("ingestion_date", current_timestamp())  \
                                            .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only required columns

# COMMAND ----------

races_final_df = races_with_timestamp_df.select("race_id","race_year","round","circuit_id","name","ingestion_date","race_timestamp").withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("success")

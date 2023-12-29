# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId",IntegerType(),False),\
                                     StructField("driverId",IntegerType(),True),\
                                     StructField("stop",StringType(),True),\
                                     StructField("lap",IntegerType(),True),\
                                     StructField("time",StringType(),True),\
                                     StructField("duration",StringType(),True),\
                                     StructField("milliseconds",IntegerType(),True)
                                     
])

# COMMAND ----------

pit_stop_df = spark.read.schema(pit_stop_schema).option("multiline",True).json(f"{bronze_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stops_final_df = pit_stop_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("ingestion_date",current_timestamp())\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

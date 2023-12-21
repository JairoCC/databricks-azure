# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_timestamp

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

pit_stop_df = spark.read.schema(pit_stop_schema).option("multiline",True).json("/mnt/formula1dljc/bronze/pit_stops.json")

# COMMAND ----------

pit_stops_final_df = pit_stop_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formula1dljc/silver/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("success")

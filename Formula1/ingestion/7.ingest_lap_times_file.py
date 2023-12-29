# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the CVS file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),\
                                     StructField("driverId",IntegerType(),True),\
                                     StructField("stop",StringType(),True),\
                                     StructField("lap",IntegerType(),True),\
                                     StructField("position",IntegerType(),True),\
                                     StructField("time",StringType(),True),\
                                     StructField("milliseconds",IntegerType(),True)
                                     
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{bronze_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("ingestion_date",current_timestamp())\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

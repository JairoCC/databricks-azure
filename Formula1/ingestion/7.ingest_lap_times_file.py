# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the CVS file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_timestamp

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

lap_times_df = spark.read.schema(lap_times_schema).csv("/mnt/formula1dljc/bronze/lap_times")

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1dljc/silver/lap_times")

# COMMAND ----------

dbutils.notebook.exit("success")

# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),\
                                    StructField("raceId",IntegerType(),True),\
                                    StructField("driverId",IntegerType(),True),\
                                    StructField("constructorId",IntegerType(),True),\
                                    StructField("number",IntegerType(),True),\
                                    StructField("grid",IntegerType(),True),\
                                    StructField("position",IntegerType(),True),\
                                    StructField("positionText",StringType(),True),\
                                    StructField("positionOrder",IntegerType(),True),\
                                    StructField("points",FloatType(),True),\
                                    StructField("laps",IntegerType(),True),\
                                    StructField("time",StringType(),True),\
                                    StructField("milliseconds",IntegerType(),True),\
                                    StructField("fastestLap",IntegerType(),True),\
                                    StructField("rank",IntegerType(),True),\
                                    StructField("fastestLapTime",StringType(),True),\
                                    StructField("fastestLapSpeed",StringType(),True),\
                                    StructField("statusId",IntegerType(),True)
                                                                         
])

# COMMAND ----------

result_df = spark.read.schema(results_schema).json(f"{bronze_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_final_df = result_df.withColumnRenamed("resultId","result_id")\
                            .withColumnRenamed("raceId","race_id")\
                            .withColumnRenamed("driverId","driver_id")\
                            .withColumnRenamed("constructorId","constructor_id")\
                            .withColumnRenamed("positionText","position_text")\
                            .withColumnRenamed("positionOrder","position_order")\
                            .withColumnRenamed("fastestLap","fastest_lap")\
                            .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                            .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                            .withColumn("ingestion_date",current_timestamp())\
                            .withColumn("file_date", lit(v_file_date))\
                            .drop("statusId")

# COMMAND ----------

#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

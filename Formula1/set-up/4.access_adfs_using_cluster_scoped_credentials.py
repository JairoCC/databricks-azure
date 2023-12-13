# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access Asure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dljc.dfs.core.windows.net",
               "H3tbAiiKIv/RaS8gaNCePL8xdyDmeX4GbgDFzXvZEICyV3JIvTGO9yaWBFkYqR0+1QVsQIbngjcW+AStB2hrDA==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dljc.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dljc.dfs.core.windows.net/circuits.csv"))

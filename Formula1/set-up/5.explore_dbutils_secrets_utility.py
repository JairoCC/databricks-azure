# Databricks notebook source
# MAGIC %md
# MAGIC Explore capabilities of the dbutilis secrets utilities

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dll-accounut-key')

# COMMAND ----------



-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT *, concat(driver_Ref, '-', code) AS new_driver_ref FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, split(name, ' ') [0] forename,  split(name, ' ') [1] surname
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, current_timestamp()
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy')
FROM f1_processed.drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## SQL Aggregations and Window functions

-- COMMAND ----------

SELECT count(*) FROM f1_processed.drivers;

-- COMMAND ----------

SELECT max(dob) FROM f1_processed.drivers;

-- COMMAND ----------

SELECT count(*) FROM f1_processed.drivers
WHERE nationality = 'British';

-- COMMAND ----------

SELECT nationality, count(*) FROM f1_processed.drivers
GROUP BY nationality
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, count(*) FROM f1_processed.drivers
GROUP BY nationality
HAVING count(*) > 100
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, name, dob, rank() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM f1_processed.drivers
ORDER BY nationality, age_rank;

-- COMMAND ----------



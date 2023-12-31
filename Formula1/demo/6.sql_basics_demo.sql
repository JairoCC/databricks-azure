-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers LIMIT 10;

-- COMMAND ----------

DESC f1_processed.drivers;

-- COMMAND ----------

SELECT name,dob AS date_of_birth 
  FROM f1_processed.drivers 
WHERE nationality = 'British'
  AND dob >= '1990-01-01'
ORDER BY dob DESC;

-- COMMAND ----------

SELECT name,dob AS date_of_birth, nationality 
  FROM f1_processed.drivers 
WHERE (nationality = 'British'
  AND dob >= '1990-01-01') OR nationality = 'Indian'
ORDER BY dob DESC;

-- COMMAND ----------



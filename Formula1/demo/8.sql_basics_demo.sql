-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

DESC driver_standing;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2017
AS
SELECT race_year, driver_name, team, total_points, wins, rank
 FROM f1_presentation.driver_standing
WHERE race_year = 2017; 

-- COMMAND ----------

SELECT * FROM v_driver_standings_2017;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
 FROM f1_presentation.driver_standing
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018;

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
JOIN v_driver_standings_2017 d_2017
ON (d_2018.driver_name = d_2017.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Left join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2017 d_2017
ON (d_2018.driver_name = d_2017.driver_name);

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2017 d_2017
ON (d_2018.driver_name = d_2017.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Full Join

-- COMMAND ----------

c

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Semi Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2017 d_2017
ON (d_2018.driver_name = d_2017.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2017 d_2017
ON (d_2018.driver_name = d_2017.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cross Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2017 d_2017;

-- COMMAND ----------



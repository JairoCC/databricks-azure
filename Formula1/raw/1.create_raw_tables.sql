-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(circuitsId INT,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
) 
USING csv
OPTIONS (path "/mnt/formula1dljc/bronze/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
) 
USING csv
OPTIONS (path "/mnt/formula1dljc/bronze/races.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
) 
USING json
OPTIONS (path "/mnt/formula1dljc/bronze/constructors.json", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
) 
USING json
OPTIONS (path "/mnt/formula1dljc/bronze/drivers.json", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId INT
) 
USING json
OPTIONS (path "/mnt/formula1dljc/bronze/results.json", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit stops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
) 
USING json
OPTIONS (path "/mnt/formula1dljc/bronze/pit_stops.json", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap times table
-- MAGIC - csv file
-- MAGIC - multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
) 
USING csv
OPTIONS (path "/mnt/formula1dljc/bronze/lap_times");

-- COMMAND ----------

SELECT COUNT(1) FROM f1_raw.lap_times;

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create qualifying table
-- MAGIC - JSON file
-- MAGIC - Multipline JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyingId INT,
raceId INT
) 
USING json
OPTIONS (path "/mnt/formula1dljc/bronze/qualifying", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------



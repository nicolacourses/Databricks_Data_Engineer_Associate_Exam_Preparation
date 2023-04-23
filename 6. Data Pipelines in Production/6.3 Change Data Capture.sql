-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Change Data Capture
-- MAGIC In this video, we will apply concepts regarding Delta Live Tables we are already familiar with, incorporating Change Data Capture syntax and data objects.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC Once completed this video, we should be able to:
-- MAGIC - Work with Change Data Capture syntax in Delta Live Tables
-- MAGIC - Perform upsert and deletes operations with Change Data Capture
-- MAGIC 
-- MAGIC ## Documentation References
-- MAGIC - <a href="https://www.databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html" target="_blank">How to Simplify CDC With Delta Lake's Change Data Feed</a>
-- MAGIC - <a href="https://docs.databricks.com/delta-live-tables/cdc.html" target="_blank">Change data capture with Delta Live Tables</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Bronze tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_opera_artists_raw
COMMENT "The raw products files ingested from products_files via Auto Loader"
AS SELECT * FROM cloud_files("dbfs:/_source_system_opera_artists_files",
                              "parquet",
                              map("schema", "artist_id INT, artist_name STRING, artist_surname STRING, repertoire STRING, operation_date TIMESTAMP, operation STRING"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_opera_artists;

APPLY CHANGES INTO LIVE.silver_opera_artists
  FROM STREAM(LIVE.bronze_opera_artists_raw)
  KEYS (artist_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date
  COLUMNS * EXCEPT (operation, operation_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Gold tables

-- COMMAND ----------

CREATE LIVE TABLE repertoire_count
COMMENT "Number of product per product category"
AS SELECT repertoire, COUNT(*) AS count_of_opera_artists
FROM LIVE.silver_opera_artists
GROUP BY repertoire;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC In this video, we incorporate Change Data Capture in a simple Delta Live Tables workflow. We went through the necessary syntax in details, and we had a look at how upsert and deletes operations are handled by Apply Changes Into in practice. Finally, we once again went back to our Delta Live Tables UI.
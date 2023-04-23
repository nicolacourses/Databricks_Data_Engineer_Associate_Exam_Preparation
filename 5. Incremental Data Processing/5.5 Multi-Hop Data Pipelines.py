# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Multi-Hop Data Pipelines
# MAGIC In this video, we will implement a multi-hop architecture. Our goal is moving from raw data with low business value, to polished data with high business value. We will start by ingesting files incrementally via Auto Loader and then we will proceed through the layers.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Describe the multi-layer nature of a Multi-Hop architecture (Bronze, Silver, Gold)
# MAGIC - Create a Delta Lake multi-hop data pipeline
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://www.databricks.com/glossary/medallion-architecture" target="_blank">What is a Multi-Hop or Medallion architecture ?</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Bronze Layer: our raw data
# MAGIC The bronze layer includes files from which we want to fetch our initial data. In this case we have:
# MAGIC - A parquet file with a first load of artists
# MAGIC - A parquet file with a second load of artists
# MAGIC - A csv file with artist producers
# MAGIC - A csv file with artist albums
# MAGIC 
# MAGIC We will first load our artists parquet files into our target temporary view via Auto Loader. In a real world scenario, over time, new artists parquet files would end up in our source folder as parquet files. Eventually, they would be automatically, incrementally, added to our target Delta table.
# MAGIC 
# MAGIC As a further step, we will add useful metadata to describe the parquet files content. This step can be helpful to troubleshoot data going forward. To do so, we will use a temporary view. We will then read back the data, with the metadata, and write them to a Delta table.

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/_source_system_artists_files')

dbutils.fs.cp('dbfs:/FileStore/data_files/artists_first_load.parquet',
              'dbfs:/_source_system_artists_files', True)

# COMMAND ----------

(spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", "dbfs:/_artists_stream_checkpoint")
      .load("dbfs:/_source_system_artists_files")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/_artists_stream_checkpoint")
      .outputMode("append")
      .table("bronze_artists"));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_artists;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_artists;

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/_source_system_artists_files')

dbutils.fs.cp('dbfs:/FileStore/data_files/artists_second_load.parquet',
              'dbfs:/_source_system_artists_files', True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Silver Layer: our enriched data
# MAGIC The silver layer includes files with additional information and selected data. In this case we have:
# MAGIC - Expanded attributes via information brought in from lookup tables
# MAGIC - Filtered data according to existing attributes
# MAGIC 
# MAGIC We will first load our artist_producers and artist_album to two different temporary views by querying the files directly. We are assuming that these files are static, and get replaced in our storage source in their entirety should they be updated. 
# MAGIC 
# MAGIC We will then join these two files to the initial artists temporary view on their respective keys, augmenting the information provided by each record. Right after, we will filter records according to some conditions. The idea is creating a table with enriched, filtered data, with its records ready to be aggregated.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_bronze_artists_albums
# MAGIC   (album_id INT,
# MAGIC    album_name STRING,
# MAGIC    album_genre STRING,
# MAGIC    release_date DATE,
# MAGIC    album_sales_in_million INT
# MAGIC    )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   path = "dbfs:/FileStore/data_files/artist_album.csv",
# MAGIC   delimiter = ","
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_vw_bronze_artists_albums;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_bronze_artists_producers
# MAGIC   (producer_id INT,
# MAGIC    producer_name STRING,
# MAGIC    producer_country STRING,
# MAGIC    producer_type STRING,
# MAGIC    first_project_date DATE,
# MAGIC    last_project_date DATE
# MAGIC    )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   path = "dbfs:/FileStore/data_files/artist_producers.csv",
# MAGIC   delimiter = ","
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_vw_bronze_artists_producers;

# COMMAND ----------

(spark
  .readStream
  .table("bronze_artists")
  .createOrReplaceTempView("streaming_bronze_artists"));

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_silver_artists AS
# MAGIC SELECT
# MAGIC      BA.artist_id,
# MAGIC      BA.artist_name,
# MAGIC      BA.artist_genre,
# MAGIC      BA.artist_country,
# MAGIC      VWBA.album_name,
# MAGIC      VWBA.album_genre,
# MAGIC      VWBA.release_date AS album_release_date,
# MAGIC      VWBA.album_sales_in_million,
# MAGIC      VWBP.producer_name,
# MAGIC      VWBP.producer_country,
# MAGIC      VWBP.producer_type,
# MAGIC      VWBP.first_project_date,
# MAGIC      VWBP.last_project_date
# MAGIC FROM streaming_bronze_artists AS BA
# MAGIC      LEFT JOIN temp_vw_bronze_artists_producers AS VWBP
# MAGIC      ON BA.producer_id = VWBP.producer_id
# MAGIC      LEFT JOIN temp_vw_bronze_artists_albums AS VWBA
# MAGIC      ON BA.album_id = VWBA.album_id;

# COMMAND ----------

(spark.table("temp_vw_silver_artists")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/_silver_artists_stream_checkpoint")
      .outputMode("append")
      .table("silver_artists"));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_artists;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Gold Layer: our business data
# MAGIC The gold layer includes final tables with data with the highest business value, ready to be aggregated to return valuable insights. In this case we have:
# MAGIC - Aggregated records to return specific metrics per artist_genre and create a Delta table
# MAGIC - Aggregated records to return specific metrics per artist_producer and create a Delta table
# MAGIC 
# MAGIC The two gold Delta tables are now ready to support BI or ML workloads and be refreshed over time.

# COMMAND ----------

(spark.readStream
  .table("silver_artists")
  .createOrReplaceTempView("temp_vw_silver_artists_to_be_aggregated"));

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_gold_artist_genre_metrics AS
# MAGIC SELECT 
# MAGIC       artist_genre,
# MAGIC       COUNT(album_name) AS count_of_albums,
# MAGIC       SUM(album_sales_in_million) AS total_sales_in_million,
# MAGIC       CAST(( SUM(album_sales_in_million) / COUNT(album_name)) AS DECIMAL ) AS average_sales_per_album_in_million
# MAGIC FROM temp_vw_silver_artists_to_be_aggregated
# MAGIC GROUP BY artist_genre;

# COMMAND ----------

(spark.table("temp_vw_gold_artist_genre_metrics")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/_gold_artists_genre_metrics_stream_checkpoint")
      .trigger(availableNow=True)
      .table("gold_artist_genre_metrics"));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_artist_genre_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_gold_artist_producers_metrics AS
# MAGIC SELECT 
# MAGIC       producer_name,
# MAGIC       COUNT(album_name) AS count_of_albums,
# MAGIC       SUM(album_sales_in_million) AS total_sales_in_million,
# MAGIC       CAST(( SUM(album_sales_in_million) / COUNT(album_name)) AS DECIMAL ) AS average_sales_per_album_in_million
# MAGIC FROM temp_vw_silver_artists_to_be_aggregated
# MAGIC GROUP BY producer_name;

# COMMAND ----------

(spark.table("temp_vw_gold_artist_producers_metrics")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/_gold_artists_producers_metrics_stream_checkpoint")
      .trigger(availableNow=True)
      .table("gold_artist_producers_metrics"));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_artist_producers_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

paths = [
        "dbfs:/_artists_stream_checkpoint",
        "dbfs:/_gold_artists_genre_metrics_stream_checkpoint",
        "dbfs:/_gold_artists_producers_metrics_stream_checkpoint",
        "dbfs:/_silver_artists_stream_checkpoint",
        "dbfs:/_source_system_artists_files",
        "dbfs:/local_disk0"
        ]

for p in paths:
    dbutils.fs.rm(p, True)

# COMMAND ----------

paths = [
        "dbfs:/_source_system_artists_files",
        "dbfs:/pipelines"
        ]

for p in paths:
    dbutils.fs.rm(p, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_artists;
# MAGIC DROP TABLE IF EXISTS silver_artists;
# MAGIC DROP TABLE IF EXISTS gold_artist_genre_metrics;
# MAGIC DROP TABLE IF EXISTS gold_artist_producers_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we implemented a multi-hop (or medallion) architecture. We started with a few parquet files of raw data loaded via Auto Loader in a target streaming Delta table, adding some useful metadata. We then expanded the data business value by adding information from static csv files. Finally, we created data assets, in the gold layer, ready to support BI or ML workloads. These data assets have the highest business value.
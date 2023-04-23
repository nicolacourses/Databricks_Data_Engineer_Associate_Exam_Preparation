-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Delta Live Tables
-- MAGIC In this video, we will get familiar with how to declare (build) a Delta Live Tables workflow using its declarative approach in SparkSQL. To do so, we will start from our previous Multi-Hop architecture implementation.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC Once completed this video, we should be able to:
-- MAGIC - Be knowledgeable about Delta Live Tables syntax
-- MAGIC - Declare a Delta Live Tables workflow
-- MAGIC - Use Auto Loader as part of a Delta Live Tables workflow
-- MAGIC - Understand how to handle static and streaming Delta Live Tables
-- MAGIC 
-- MAGIC ## Documentation References
-- MAGIC - <a href="https://www.databricks.com/product/delta-live-tables" target="_blank">Delta Live Tables</a>
-- MAGIC - <a href="https://docs.databricks.com/delta-live-tables/index.html" target="_blank">What is Delta Live Tables ?</a>
-- MAGIC - <a href="https://docs.databricks.com/delta-live-tables/tutorial-sql.html" target="_blank">Declare a data pipeline with SQL in Delta Live Tables</a>
-- MAGIC - <a href="https://docs.databricks.com/delta-live-tables/tutorial-python.html" target="_blank">Declare a data pipeline with Python in Delta Live Tables</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Delta Live Tables at work
-- MAGIC In the following cells we separate our declarations with respect to bronze tables, silver tables, and gold tables. The code comes from the Multi-Hop architecture we implemented in the previous videos. When working with Delta Live Tables:
-- MAGIC - The **LIVE** keyword always precedes Delta Live Tables
-- MAGIC - The **STREAMING** and **cloud_files** keywords are required to apply an incremental data ingestion with Auto Loader
-- MAGIC 
-- MAGIC It is important to note that the **cloud_files()** method is what incorporates Auto Loader natively in SQL. In particular, it works with the following parameters:
-- MAGIC - The files source location
-- MAGIC - The files source data format
-- MAGIC - A number of optional reader options
-- MAGIC 
-- MAGIC Finally, running a cell with Delta Live Tables declarations will just check its syntax. To set up a workflow properly, it is mandatory to create an actual data pipeline via the UI.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Bronze tables
-- MAGIC The following cells will import the raw data, either via Auto Loader by processing files incrementally, or by importing simpler static lookup files.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_artists_raw
COMMENT "The raw artists files ingested from artists_files via Auto Loader"
AS SELECT * FROM cloud_files("dbfs:/_source_system_artists_files/",
                             "parquet",
                              map("schema", "artist_id INT, artist_name STRING, artist_genre STRING, artist_country STRING, album_id INT, producer_id INT"));

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_artists_producers_raw
COMMENT "The raw artists producers lookup file"
AS SELECT * FROM parquet.`dbfs:/FileStore/data_files/artist_producers.parquet`;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_artists_album_raw
COMMENT "The raw artists producers lookup file"
AS SELECT * FROM parquet.`dbfs:/FileStore/data_files/artist_album.parquet`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver tables
-- MAGIC The following cells will build make sure to have cleaner data in place. In this respect, Delta Live Tables provide us with a few options to enforce a higher degree of data quality. In particular:
-- MAGIC - The **CONSTRAINT** keyword enforces data quality constraints and collects insights on constraints violations
-- MAGIC - The option **ON VIOLATION** clause triggers an action if a given record violates a given constraint
-- MAGIC 
-- MAGIC Regarding the last point, Delta Live Tables allow for the following actions:
-- MAGIC - If no action is specified, records violating a constraint will be loaded to a table, but violations will be reported in Delta Live Tables data quality metrics
-- MAGIC - **FAIL UPDATE** causes the pipeline to fail when a constraint is violated
-- MAGIC - **DROP ROW** drops records that violate a given contraint, while the pipeline will go on

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_artists (
     CONSTRAINT valid_artist_id EXPECT (artist_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Each and every artist must have a valid artist_id value, as artist_id is a primary key" 
AS
SELECT
     BA.artist_id,
     BA.artist_name,
     BA.artist_genre,
     BA.artist_country,
     VWBA.album_name,
     VWBA.album_genre,
     VWBA.release_date AS album_release_date,
     VWBA.album_sales_in_million,
     VWBP.producer_name,
     VWBP.producer_country,
     VWBP.producer_type,
     VWBP.first_project_date,
     VWBP.last_project_date
FROM STREAM(LIVE.bronze_artists_raw) AS BA
     LEFT JOIN LIVE.bronze_artists_producers_raw AS VWBP
     ON BA.producer_id = VWBP.producer_id
     LEFT JOIN LIVE.bronze_artists_album_raw AS VWBA
     ON BA.album_id = VWBA.album_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Gold tables
-- MAGIC The following cells will finally create the data assets with the highest business value to support Bi or ML workloads and use cases.
-- MAGIC 
-- MAGIC After defining the gold live tables, we can finally explored the Directed Acyclic Graph (DAG) which visualizes:
-- MAGIC - The data objects, for instance tables, part of the pipeline
-- MAGIC - The respective dependencies
-- MAGIC - The data lineage
-- MAGIC 
-- MAGIC In addition, each table will return useful information, such as:
-- MAGIC - Run status
-- MAGIC - Metadata summary
-- MAGIC - Schema
-- MAGIC - Data quality metrics

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_artist_genre_metrics
COMMENT "Artist metrics per genre"
AS
SELECT 
      artist_genre,
      COUNT(album_name) AS count_of_albums,
      SUM(album_sales_in_million) AS total_sales_in_million,
      CAST(( SUM(album_sales_in_million) / COUNT(album_name)) AS DECIMAL ) AS average_sales_per_album_in_million
FROM LIVE.silver_artists
GROUP BY artist_genre;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_artist_producers_metrics
COMMENT "Artist metrics per album"
SELECT 
      producer_name,
      COUNT(album_name) AS count_of_albums,
      SUM(album_sales_in_million) AS total_sales_in_million,
      CAST(( SUM(album_sales_in_million) / COUNT(album_name)) AS DECIMAL ) AS average_sales_per_album_in_million
FROM LIVE.silver_artists
GROUP BY producer_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC In this video, we became familiar with the syntax to define a Delta Live Tables workflow, including both static and streaming tables. We introduces some of the features which allow for better data quality and monitoring. Finally, we presented the Delta Live Tables UI, and the useful information provided per each data object (tables) involved in a Delta Live Tables workflow.
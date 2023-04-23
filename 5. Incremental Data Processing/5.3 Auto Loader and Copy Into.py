# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Auto Loader and Copy Into
# MAGIC In this video, we will get familiar with ingesting files incrementally with Auto Loader and Copy Into. This is arguably an important operation with respect to ingesting data from cloud object storage solutions, such as a data lake.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC - Ingest files incrementally to Delta tables using Auto Loader
# MAGIC - Ingest files incrementally to Delta tables using Copy Into
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/ingestion/auto-loader/index.html" target="_blank">What is Auto Loader?</a>
# MAGIC - <a href="https://docs.databricks.com/ingestion/auto-loader/options.html" target="_blank">Auto Loader options</a>
# MAGIC - <a href="https://docs.databricks.com/ingestion/auto-loader/schema.html" target="_blank">Configure schema inference and evolution in Auto Loader</a>
# MAGIC - <a href="https://docs.databricks.com/ingestion/copy-into/index.html" target="_blank">Load Data with COPY INTO</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Auto Loader in action
# MAGIC In the following cell we define a script to demonstrate the use of Auto Loader in Databricks, reading and writing data from a source to a target Delta table. We will then describe the history of our target Delta table to better identify the writing operations performed via Auto Loader.
# MAGIC 
# MAGIC Since Auto Loader relies on Structured Streaming:
# MAGIC - The next cell will incorporate a continuously active query
# MAGIC - The next cell will not stop running (as with a static query), unless we stop it manually

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/_source_system_purchases_files')

dbutils.fs.cp('dbfs:/FileStore/data_files/purchases_load_one.parquet',
              'dbfs:/_source_system_purchases_files', True)

# COMMAND ----------

(spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", "dbfs:/_purchases_stream_checkpoint_read")
      .load("dbfs:/_source_system_purchases_files")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/_purchases_stream_checkpoint_write")
      .outputMode("append")
      .table("target_delta_table"));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM target_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY target_delta_table;

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/data_files/purchases_load_two.parquet',
              'dbfs:/_source_system_purchases_files', True);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Copy Into in action
# MAGIC 
# MAGIC We can use the **COPY INTO** statement to ingest data from external systems, incrementally. In particular:
# MAGIC - It is much faster than full table scans
# MAGIC - It comes in handy when we have to work with a limite number of files (not a million or a billion)
# MAGIC - It does not handle duplicated records, much like an **INSERT INTO**
# MAGIC - The source files or tables schemas should be consistent over time

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM target_delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO target_delta_table
# MAGIC FROM "dbfs:/_source_system_purchases_files"
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM target_delta_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm("dbfs:/_purchases_stream_checkpoint_read" ,True)
dbutils.fs.rm("dbfs:/_purchases_stream_checkpoint_write" ,True)
dbutils.fs.rm("dbfs:/_source_system_purchases_files" ,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS target_delta_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we introduced the PySpark script with which we can enable Auto Loader and we became familiar with its syntax. We then ingested parquet files incrementally, from a source folder, into a target streaming Delta table. Finally, we had a look at how does the history of the target object change every time an Auto Loader ingests data, and we introduced the syntax and results of the COPY INTO command.
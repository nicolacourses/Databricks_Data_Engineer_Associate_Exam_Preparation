# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Structured Streaming
# MAGIC In this video, we will have a look at working with Structured Streaming and how can we use it to process data coming datasets that can potentially grow indefinitely over time.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Read data from a data source
# MAGIC - Write data to a target storage
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/structured-streaming/index.html" target="_blank">What is Apache Spark Structured Streaming</a>
# MAGIC - <a href="https://docs.databricks.com/structured-streaming/production.html" target="_blank">Production considerations for Structured Streaming</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading data from a data stream
# MAGIC 
# MAGIC We can use **spark.readStream()** to query a data stream.
# MAGIC 
# MAGIC In this example we will first create a Delta Lake table as our data source. We will then write a query in PySpark to read the records stored in that table, incrementally, in a temporary view. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_parquet AS
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sales_parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_parquet;

# COMMAND ----------

(
    spark
    .readStream
    .table("sales_parquet")
    .createOrReplaceTempView("streaming_vw_tmp_sales_parquet")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_vw_tmp_sales_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SparkSQL queries on Streaming Data
# MAGIC As mentioned, we can query a streaming temporary view as we would query a static temprary view. Most operations are supported, with the exception of a few, more complex unsupported operations.
# MAGIC 
# MAGIC Every time that we query a streaming temporary view, that query becomes a streaming query, meaning that it will be executed indefinitely.
# MAGIC 
# MAGIC Let's remember to stop each and every streaming query, after we execute it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW streaming_vw_temp_products_count AS
# MAGIC (
# MAGIC SELECT product, count(product) AS count_of_product
# MAGIC FROM streaming_vw_tmp_sales_parquet
# MAGIC GROUP BY product
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_vw_temp_products_count;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT * FROM streaming_vw_temp_products_count;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Writing data from a data stream
# MAGIC 
# MAGIC We can use **`spark.writeStream()`** to persist the output of a streaming query to a storage object, or a database object, such as a Delta Table. In our example, we will use the trigger method **availableNow = True** and the outputMode **complete**.

# COMMAND ----------

(
    spark
    .table("streaming_vw_temp_products_count")                               
    .writeStream                                                
    .option("checkpointLocation", "dbfs:/_products_checkpoint")
    .outputMode("complete")
    .trigger(availableNow=True)
    .table("streaming_products_count")
);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_products_count;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED streaming_products_count;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm("dbfs:/_products_checkpoint", True)
dbutils.fs.rm("dbfs:/local_disk0", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_parquet;
# MAGIC DROP TABLE IF EXISTS streaming_products_count;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we initially looked into reading a stream of data from a data source. We then got familiar with how to configure a script to write a stream of data to a storage object, such as a Delta table. We also had an example of supported and unsupported SparkSQL operations on streaming datasets.
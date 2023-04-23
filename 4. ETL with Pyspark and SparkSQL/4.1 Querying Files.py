# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Querying Files
# MAGIC In this video, we will learn how to query and fetch data from files using SparkSQL statements.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Query files directly with SparkSQL
# MAGIC - Query files with **text** and **binaryFile** methods
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/data-governance/unity-catalog/queries.html#select-from-files" target="_blank">Query Data - Select from Files</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Query files directly with SparkSQL
# MAGIC 
# MAGIC To query data in a single file, we can follow this pattern: **SELECT * FROM file_format.&#x60;/path/to/file&#x60;**.
# MAGIC A thing worth mentioning is the use of back-ticks instead of single quotes around the /path/to/file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     region,
# MAGIC     country,
# MAGIC     city
# MAGIC FROM json.`dbfs:/FileStore/geographies_first_load.json`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/data_files/artists_first_load.parquet`;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To query multiple files in a directory, assuming they have the same format and schema, we can instead specify the directory path.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     region,
# MAGIC     country,
# MAGIC     city
# MAGIC FROM json.`/FileStore`
# MAGIC WHERE id IS NOT NULL
# MAGIC       AND region IS NOT NULL
# MAGIC       AND country IS NOT NULL
# MAGIC       AND city IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In addition, we can create objects with which we incapsulate the data from one or more files. A good example can be a temporary view, with which by now we are familiar.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW geographies_temp_view
# MAGIC AS SELECT * FROM json.`dbfs:/FileStore/geographies_first_load.json`;
# MAGIC 
# MAGIC SELECT * FROM geographies_temp_view;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Query files with **text** and **binaryFile** methods
# MAGIC 
# MAGIC The **text** format allows us to load each line of a text-based file with a single string column called **value**. This can come in handy when data sources are prone to corruption and we need to apply custom functions to extract data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`dbfs:/FileStore/geographies_first_load.json`;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The **binaryFile** method allows us to query a directory and provide file metadata together with the binary representation of its contents. Regarding the metadata, the fields created will be:
# MAGIC - path
# MAGIC - modificationTime
# MAGIC - length
# MAGIC - context

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`dbfs:/FileStore/geographies_first_load.json`;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we presented how Databricks can query files directly. We briefly mentioned how we can stored data from files in a reference object, such as a view. Finally, we introduced querying files with the Text and BinaryFile methods.
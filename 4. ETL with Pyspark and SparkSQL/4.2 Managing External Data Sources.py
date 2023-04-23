# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Managing External Data Sources
# MAGIC In this video, we will create tables utilizing external data sources. In particular, we will use a CSV file.
# MAGIC 
# MAGIC These tables are unfortunately not saved in the Delta Lake format and therefore, they are not optimized for the Databricks Lakehouse. In fact, while some formats are especially suitable for being directly queried, others require additional configuration to ingest their data properly.
# MAGIC 
# MAGIC Even though in this video we are using a CSV file, the overarching concept applies to all external data sources.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Set options for fetching data from external sources with SparkSQL
# MAGIC - Create tables for external data sources for a variety of file formats
# MAGIC - Identify what the default behavior is when querying tables created for external data sources
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/external-data/index.html" target="_blank">Interact with external data on Databricks</a>
# MAGIC - <a href="https://docs.databricks.com/external-data/csv.html" target="_blank">Interact with CSV files</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set options for fetching data from external sources with SparkSQL
# MAGIC 
# MAGIC While CSV files are extensively used, directly querying these files often times does not produces the anticipated outcomes. For example:
# MAGIC - The header row is fetched as a table row
# MAGIC - Multiple columns can be extracted as a single column
# MAGIC - The datatype is *string* across the board
# MAGIC 
# MAGIC As the CSV format, other data formats will require a schema declaration, or other settings to return data according to our expectations.
# MAGIC 
# MAGIC With respect to creating views, the following syntax will do for most data formats:
# MAGIC 
# MAGIC **CREATE OR REPLACE TEMPORARY VIEW view_identifier (col_name1 col_type1, ...)<br/>
# MAGIC USING data_source<br/>
# MAGIC OPTIONS (key1 = "val1", key2 = "val2", ...);<br/>**
# MAGIC 
# MAGIC And, with respect to creating tables, the following syntax will do for most data formats:
# MAGIC 
# MAGIC **CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
# MAGIC USING data_source<br/>
# MAGIC OPTIONS (key1 = "val1", key2 = "val2", ...)<br/>
# MAGIC LOCATION = path;<br/>**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`dbfs:/FileStore/companies.csv`;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The following cell uses SparkSQL to create a view for a .csv source, where the following is specified:
# MAGIC - The column names and data types
# MAGIC - The file format
# MAGIC - The .csv delimiter
# MAGIC - The presence of a header
# MAGIC - The path to where the .csv is stored
# MAGIC 
# MAGIC This view will come in handy in a few cells. In addition, it represents a good example of the syntax with which we can create a view starting from an external data source.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_companies_csv
# MAGIC   (id INT,
# MAGIC    company_name STRING,
# MAGIC    business_segment STRING,
# MAGIC    business STRING
# MAGIC    )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   path = "dbfs:/FileStore/companies.csv",
# MAGIC   delimiter = "|"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_vw_companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With the following cell, we are now exporting the content of our view as a .csv file to the csv_tables directory.
# MAGIC 
# MAGIC The idea is creating the behavior of a real-world scenario, where periodically .csv files are added to a given directory.
# MAGIC 
# MAGIC In this example, in place of the .csv file, we have our temporary view.

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.table("temp_vw_companies_csv")
# MAGIC       .write
# MAGIC       .format("csv")
# MAGIC       .save("dbfs:/_source_system_one/_csv_files/"));

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell uses SparkSQL to create a table for a .csv source, declaring:
# MAGIC - The column names and data types
# MAGIC - The file format
# MAGIC - The path where the .csv is stored
# MAGIC 
# MAGIC This is an external table, built on an external sources, without the benefits of Delta Lake tables.
# MAGIC 
# MAGIC In this case, our source is the csv file created from our view. In our view, we already declared the header and the delimiter. Therefore, there is no need to do the same thing again while creating the table.
# MAGIC 
# MAGIC No data is relocated while the table is created. Just like when we conducted a direct query on our files and created a view, we are only indicating files that are saved in an external location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE companies_csv
# MAGIC   (id INT,
# MAGIC    company_name STRING,
# MAGIC    business_segment STRING,
# MAGIC    business STRING
# MAGIC    )
# MAGIC USING CSV
# MAGIC LOCATION "dbfs:/_source_system_one/_csv_files/";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM companies_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Every piece of metadata specified while creating the external table will be saved to the Hive central metastore. This guarantees that data in that location will always be accessed with those options.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Limits of Tables with External Data Sources
# MAGIC When we create tables or queries that depend on external data sources, we should assume that the Delta Lake benefits will not hold.
# MAGIC 
# MAGIC As an example, a Delta Lake table provides assurance that we will always retrieve the most current status of our source data, thanks to its JSON log file. However, tables created with data sources that are not particularly suited to be queried directly might reflect previously cached versions.
# MAGIC 
# MAGIC In the following example, we will:
# MAGIC - Append new records to our table created with an external data source
# MAGIC - Query the table and verify that the records count does not reflect the append operation
# MAGIC - Refresh Spark cache manually to include the append operation
# MAGIC 
# MAGIC In particular, when we first created our companies_csv table in this notebook, Spark automatically stored the underlying data in the local storage. This guarantees that subsequent queries will be optimized by only retrieving data from this local cache.
# MAGIC 
# MAGIC The cell here below will simulate the addition of a new .csv file to our data source, that is, the csv_tables directory.

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.table("temp_vw_companies_csv")
# MAGIC       .write
# MAGIC       .mode("append")
# MAGIC       .format("csv")
# MAGIC       .save("dbfs:/_source_system_one/_csv_files/"));

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The problem that we have now is, our external data source is not built to notify Spark to update its local cache. Here we would expect 40 records, while in fact we still have 20.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC However, it is possible to manually update the cache by executing the **REFRESH TABLE** command.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Here we would now expecet to see 40 records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM companies_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm("dbfs:/_source_system_one", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE companies_csv;
# MAGIC DROP VIEW temp_vw_companies_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we explored the syntax, and the options especially, with which we can create database objects from an external data source in Databricks. In particular, we noticed that external (unamanged) tables do not benefit from the advantages of Delta tables. We then demonstrated the limitations of our external (unmanaged) table with respect to the Spark cache.
# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Databases and Tables
# MAGIC In this video, we will create and manage databases and tables in Databricks.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Run databases SparkSQL DDL statements
# MAGIC - Run tables SparkSQL DDL statements
# MAGIC - Understand in detail the impact of the **LOCATION** argument
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Database objects in Databricks</a>
# MAGIC - <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and External (Unmanaged) Tables</a>
# MAGIC - <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
# MAGIC - <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run Databases SparkSQL DDL statements
# MAGIC We can start by creating one database without using the **LOCATION** statement, and a second database using it.
# MAGIC 
# MAGIC The second database will refer to a directory, to a specific storage, whose location is specified using the **LOCATION** keyword.
# MAGIC 
# MAGIC On the other hand, the first database will refer to a default location in Databricks, identified by the following path: **dbfs:/user/hive/warehouse/**. The directory for the database is named after the database itself and has a **.db** extension.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS database_default_location;
# MAGIC CREATE DATABASE IF NOT EXISTS database_custom_location LOCATION 'dbfs:/main/course/external_default.db';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED database_default_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED database_custom_location;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, we will proceed with creating a table within database_default_location and populating it with data. In this case we must provide the schema explicitly, as there is no existing data to deduce it from.
# MAGIC 
# MAGIC To locate the table's position, we can examine the comprehensive table description.
# MAGIC 
# MAGIC By default, any managed tables will be created in the directory **dbfs:/user/hive/warehouse/database_default_location.db/**.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE database_default_location;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS managed_table_default_db (id INT, name STRING, salary DOUBLE);
# MAGIC 
# MAGIC INSERT INTO managed_table_default_db
# MAGIC VALUES (1, 'Nicola', 40000);
# MAGIC 
# MAGIC SELECT * FROM managed_table_default_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_table_default_db;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As expected, the information and metadata of our Delta Table are stored in the default location.
# MAGIC 
# MAGIC It is important to remember that this is a *managed table*. As soon as we drop it, both the data and the metadata will be permanently removed from Delta Lake.

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/database_default_location.db/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can now go ahead and create a table in database_custom_location, the database which we created while specifying the **LOCATION** keyword. 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE database_custom_location;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS managed_table_custom_db (id INT, name STRING, salary DOUBLE);
# MAGIC 
# MAGIC INSERT INTO managed_table_custom_db
# MAGIC VALUES (1, 'Nicola', 40000);
# MAGIC 
# MAGIC SELECT * FROM managed_table_custom_db;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Once again, we can check the table metadata to find its location. As this table is created in the database_custom_location, our expectation is that the data and the metadata are persisted in the directory we specified.
# MAGIC 
# MAGIC In addition, please remember that this is always a *managed table*. As soon as we drop it, both the data and the metadata will be permanently removed from Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_table_custom_db;

# COMMAND ----------

# MAGIC %fs ls dbfs:/main/course/external_default.db

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run Tables SparkSQL DDL statements
# MAGIC 
# MAGIC We will create an external (unmanaged) table from sample data. In this case the sample data will come from a .csv file. The idea is creating a table in Delta lake and specifying a **LOCATION** keyword to include the path in which we have our data source.
# MAGIC 
# MAGIC Later, we will delete the table. Please remember that this is an *external (unmanaged)* table. Only the metadata will be removed, while the underlying data will be preserved in our storage location.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE database_custom_location;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_people
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = 'dbfs:/FileStore/people.csv',
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC );
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE unmanaged_external_table LOCATION 'dbfs:/main/course/external_default.db' AS
# MAGIC SELECT * FROM temp_vw_people;
# MAGIC 
# MAGIC SELECT * FROM unmanaged_external_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED unmanaged_external_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbfs_files_paths = ["dbfs:/main", "dbfs:/user/hive/warehouse/database_default_location.db"]

for p in dbfs_files_paths:
    dbutils.fs.rm(p, True);

# COMMAND ----------

# MAGIC %sql
# MAGIC USE database_default_location;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS managed_table_default_db;
# MAGIC 
# MAGIC USE database_custom_location;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS managed_table_custom_db;
# MAGIC DROP TABLE IF EXISTS unmanaged_external_table;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS managed_table_custom_db;
# MAGIC DROP DATABASE IF EXISTS database_custom_location;
# MAGIC DROP DATABASE IF EXISTS database_default_location;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we worked with databases and one of its main objects, tables. We created databases, and specified in details the impact of the location keyword, also by describing databases metadata. Finally, we did play around with SparkSQL DDL statements for tables, creating both managed, and external (unmanaged) tables.
# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Writing to Delta tables
# MAGIC 
# MAGIC In this video, we will investigate how can we load records to Delta tables with SparkSQL statements. While several operations align with conventional SQL practices, some adjustments are necessary to cater to the unique features of Delta Lake and Spark's execution environment.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Overwrite tables records with **INSERT OVERWRITE**
# MAGIC - Append tables records with **INSERT INTO**
# MAGIC - Append, update, delete records with **MERGE INTO**
# MAGIC - Ingest data incrementally with **COPY INTO**
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/sql-ref-syntax-dml-insert-into.html" target="_blank">Insert and Insert Overwrite - Details</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/delta-merge-into.html" target="_blank">Merge Into - Details</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/delta-copy-into.html" target="_blank">Copy Into - Details</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overwrite tables records with **INSERT OVERWRITE**
# MAGIC 
# MAGIC By using overwrites, we have the ability to replace all the data in a table atomically. Utilizing this technique can lead to numerous advantages when compared to the more traditional method of deleting and recreating tables:
# MAGIC - Overwriting is much faster, as it does not need to delete any files or list directories
# MAGIC - The table's version before overwriting can be retrieved via the log file
# MAGIC - Overwriting is atomic. Concurrent queries can still read the table while you are overwriting it
# MAGIC - If an overwriting fails, the table will still exist in its previous state
# MAGIC 
# MAGIC There are two main ways to perform an overwrite:
# MAGIC - **CREATE OR REPLACE TABLE** overwrites an existing table, if it exists, every time it is executed
# MAGIC - **INSERT OVERWRITE** also overwrites and existing table
# MAGIC 
# MAGIC The diffences between the two commands here above are:
# MAGIC - **INSERT OVERWRITE** does not create a new table like **CREATE OR REPLACE TABLE**, it just overwrites records
# MAGIC - **INSERT OVERWRITE** can load records that match the current table schema, as again, it does not create a new table
# MAGIC 
# MAGIC We can deduce that a **CREATE OR REPLACE TABLE** will allow us to redefine both a table and its content. On the other hand, **INSERT OVERWRITE** will never load records not consistent with the existing schema of our target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_parquet AS
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE sales_parquet
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Append Rows
# MAGIC 
# MAGIC As we would do traditionally, we can append records atomically to an existing Delta table with **INSERT INTO**. This is of course much more efficient than overwriting a table every time.
# MAGIC 
# MAGIC At the same time, **INSERT INTO** appends the same source batch of records every time. This may lead to duplicates.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sales_parquet
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Merge
# MAGIC 
# MAGIC Upserting is an operation with which we load records incrementally, to a target table, from a data source. With Delta Lake we can use the **MERGE INTO** SparkSQL statement, where we can include inserts, updates, and deletes.
# MAGIC 
# MAGIC The syntax is as follows:
# MAGIC 
# MAGIC **MERGE INTO target AS a<br/>
# MAGIC USING source AS b<br/>
# MAGIC ON {merge_condition}<br/>
# MAGIC WHEN MATCHED THEN {matched_action}<br/>
# MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>**
# MAGIC 
# MAGIC **MERGE INTO** is quite useful in practice, as:
# MAGIC - It includes in a single statement inserts, updates, and deletes
# MAGIC - It can include multiple conditions to insert, update, or delete a record
# MAGIC - It can be modified to incorporate custom logics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_parquet AS
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_parquet_updated AS
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_parquet_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE 
# MAGIC FROM sales_parquet_updated
# MAGIC WHERE Date = "2022-01-01";
# MAGIC 
# MAGIC INSERT INTO sales_parquet_updated (Date, Product, Category, Price, Quantity, Sales)
# MAGIC VALUES ("2022-01-01", "Product C", "Category AA", 100, 2, 200);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_parquet_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO sales_parquet AS a
# MAGIC USING sales_parquet_updated AS b
# MAGIC ON a.Date = b.Date
# MAGIC WHEN MATCHED AND a.Category <> b.Category THEN
# MAGIC   UPDATE SET Product = b.Product, Category = b.Category
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm("dbfs:/_tables_from_parquet", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_parquet;
# MAGIC DROP TABLE IF EXISTS sales_parquet_augmented;
# MAGIC DROP TABLE IF EXISTS sales_parquet_updated;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC In this video, we had a look at the details of the main SparkSQL statements with which we can insert records in a Delta lake table, either as a full load or incrementally. We had a look at the differences between CREATE OR REPLACE VIEW and INSERT OVERWRITE regarding replacing records in a table. Additionally, we covered the traditional INSERT INTO statement, and the COPY INTO statement. Finally, we spent a few minutes on understanding in depth the MERGE INTO statement, with which we can perform inserts, updates, and deletes in a single SparkSQL Statement.
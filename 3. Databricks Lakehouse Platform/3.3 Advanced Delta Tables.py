# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Advanced Delta Tables
# MAGIC 
# MAGIC In this video we will introduce some features unique to Delta tables.
# MAGIC 
# MAGIC ## Learning objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Use **DESCRIBE DETAIL** to query a Delta Lake table files
# MAGIC - Use **OPTIMIZE** to compact small files and **ZORDER** to index tables
# MAGIC - Use **DESCRIBE HISTORY** and **TO VERSION AS OF** to time travel
# MAGIC - Use **VACUUM** to remove old data files
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/delta/index.html" target="_blank">What is Delta Lake ?</a>
# MAGIC - <a href="https://docs.databricks.com/delta/history.html" target="_blank">History and Time Travel</a>
# MAGIC - <a href="https://docs.databricks.com/delta/vacuum.html" target="_blank">Vacuum</a>
# MAGIC - <a href="https://docs.databricks.com/delta/optimize.html" target="_blank">Optimize</a>
# MAGIC - <a href="https://docs.databricks.com/delta/data-skipping.html" target="_blank">Data skipping and z-ordeering</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating a Delta table and performing operations against it
# MAGIC The next cell performs a few opereations on the employees Delta table. After each operation, the status of our table will change. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS employees;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS employees
# MAGIC (id INT, name STRING, surname STRING, age INT, salary DOUBLE);
# MAGIC 
# MAGIC INSERT INTO employees VALUES(1, 'Din', 'Djarik', 20, 20000);
# MAGIC INSERT INTO employees VALUES(2, 'Shane', 'Butler', 65, 50000);
# MAGIC 
# MAGIC UPDATE employees 
# MAGIC SET salary = salary + 10000
# MAGIC WHERE age > 60;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS staging_employees
# MAGIC (id INT, name STRING, surname STRING, age INT, salary DOUBLE);
# MAGIC 
# MAGIC INSERT INTO staging_employees VALUES(1, 'Din', 'Djarik', 52, 20000);
# MAGIC INSERT INTO staging_employees VALUES(2, 'Shane', 'Butler', 65, 50000);
# MAGIC INSERT INTO staging_employees VALUES(3, 'Nicola', 'Filosi', 42, 35000);
# MAGIC 
# MAGIC MERGE INTO employees AS e
# MAGIC USING staging_employees AS se
# MAGIC ON e.id=se.id
# MAGIC WHEN MATCHED
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS staging_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Use **DESCRIBE DETAIL** and **DESCRIBE EXTENDED** to explore Delta tables
# MAGIC 
# MAGIC Databricks uses a Hive metastore by default to register databases, tables, views, and database objects metadata in general.
# MAGIC 
# MAGIC **DESCRIBE DETAIL** and **DESCRIBE EXTENDED** are two commands that allow us to query important metadata about a given table. In addition, **DESCRIBE DETAIL** and **DESCRIBE EXTENDED** point us to the location of the actual table data files. We can explore it with the %fs magic command.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You will notice that the directory we will explore indeed contains data files in parquet format, and a directory named **delta_log**. The actual Delta Table records are stored in the parquet files, while the transactions logs are recorded in the **delta_log**. This is where the history of all changes is stored.
# MAGIC 
# MAGIC Each transaction results in a new JSON file being written to the Delta Table transaction log. The .crc files are just the respective JSON files checksums.

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees/_delta_log/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use **OPTIMIZE** to compact small files and **ZORDER** to index tables
# MAGIC 
# MAGIC The small data files problem happens when we are attempting to process a very high volume of small data files. Generally speaking, this should be avoided to fully exploit the power of the Spark processing engine underpinning Databricks.
# MAGIC 
# MAGIC To optimize the file size and enhance its efficiency, we can use the **OPTIMIZE** command, which combines the files to reach an optimal size, in proportion to the table's size. The **OPTIMIZE** command replaces the existing data files by merging records and rewriting the results.
# MAGIC 
# MAGIC Additionally, users can choose to specify one or multiple fields for **ZORDER** indexing while executing **OPTIMIZE**.
# MAGIC This does significantly improve he data retrieval process while filtering on specific fields by organizing the data with similar values within the files.
# MAGIC 
# MAGIC In our specific case the size of our data is small, therefore **ZORDER** won't provide any tangible benefits.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE employees
# MAGIC ZORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use **DESCRIBE HISTORY** and **TO VERSION AS OF** to time travel
# MAGIC 
# MAGIC As the transaction log stores all transactions made to the Delta Lake table, it is straightforward to review the table's history.
# MAGIC 
# MAGIC All the data files that were labeled as deleted in our transaction log, are still there. This does allow us to examine earlier versions of our table. By specifying either the integer version or a timestamp, we can execute time travel queries. We can time travel.
# MAGIC 
# MAGIC Regarding time travel, it is important to understand that we are not restoring a previous status of the table by reversing transactions against the present version. We're simply performing queries on all data files that were deemed current as of the version specified.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In addition, in Delta Lake tables we can simply rollback previously committed transactions. The **RESTORE** command allows us to do so. Please note that **RESTORE** is logged as a transaction. While it may not be possible to conceal the fact that all the records in the table were accidentally deleted, it is possible to reverse the operation and restore the table to a desired state.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE students TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use **VACUUM** to remove old data files
# MAGIC 
# MAGIC Databricks has an in-built feature that automatically eliminates outdated files from Delta Lake tables.
# MAGIC 
# MAGIC While Delta Lake versioning and time travel facilitate the examination of recent versions and the reversal of queries, preserving data files for all versions of very large tables, indefinitely, can become expensive storage wise.
# MAGIC 
# MAGIC To manually remove old data files, you can use the **VACUUM** statement. When using **VACUUM,** the default setting is to disallow the deletion of files less than seven days old to avoid the possibility of long-running operations still using any of the files to be deleted.
# MAGIC 
# MAGIC It is important to remember that if you run **VACUUM** on a Delta table, the ability to time travel back to a version preceding the specified data retention period will be lost, as the data files that would allow us to do so, are deleted.
# MAGIC 
# MAGIC In the following cells, we:
# MAGIC - Turn off a check to prevent premature deletion of data files
# MAGIC - Make sure that logging of **VACUUM** commands is enabled
# MAGIC - Use the **DRY RUN** version of vacuum to print out all records to be deleted
# MAGIC 
# MAGIC Finally, we can check the table directory to show that files have been successfully deleted with the %fs magic command.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC 
# MAGIC VACUUM employees RETAIN 0 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we introduced some more advanced operations on Delta tables. We first became familiar with describe detail and describe extended, to fetch detailed metadata regarding our target table. Then, we introduced a few optimization concepts, mentioning especially the small files problem. Finally, we introduced the concept of history in Delta tables, and explained how to time travel across different versions of our target table, while maintaining our storage efficient by eliminating old data files.
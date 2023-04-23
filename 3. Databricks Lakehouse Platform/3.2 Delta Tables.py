# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC In this video, we will explore data manipulation techniques using SQL in Databricks. If you have any previous experience, with any flavour of SQL, you will already be equipped with a solid foundation for navigating the data lakehouse.
# MAGIC 
# MAGIC Keep in mind that Delta Lake serves as the default format for all tables created using Databricks. As such, working with SQL statements in Databricks automatically implies working with Delta Lake and Delta Tables.
# MAGIC 
# MAGIC ## Learning objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Create and Delete Delta Lake tables
# MAGIC - Query data from Delta Lake tables
# MAGIC - Insert, update, delete records in Delta Lake tables
# MAGIC - Write records incrementally (upsert) in Delta Lake tables
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/delta/index.html" target="_blank">What is Delta Lake ?</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create and Delete Delta tables
# MAGIC We can create a Delta table with the **CREATE TABLE** statement, which will create an empty Delta table. To avoid incurring in an error, when running multiple times this same statement, we can add the **IF NOT EXISTS** argument.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employees
# MAGIC (id INT, name STRING, surname STRING, age INT, salary DOUBLE);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Moreover, we can delete a Delta table with the **DROP TABLE** script, which will permanently delete a Delta table. To avoid incurring in an error, when running multiple times this same statement, we can add the **IF EXISTS** argument.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Query data from Delta tables
# MAGIC 
# MAGIC The **SELECT** statement allows us to query data in a Delta Lake table.
# MAGIC 
# MAGIC It is essential to note that any read operation performed on a Delta Lake table will always deliver the latest version of that table. Furthermore, ongoing operations will never result in a deadlock situation. For example, read operations on a given table can never conflict with any other operations on that same given table. 
# MAGIC 
# MAGIC All transaction details are saved in cloud object storage together with the actual data files. Therefore, concurrent reads on Delta Lake tables are only restricted by the hard limits set by cloud vendors for their object storage capabilities.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employees
# MAGIC (id INT, name STRING, surname STRING, age INT, salary DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Insert, Update, Delete records in Delta tables
# MAGIC The **INSERT INTO** statement allows us to insert values directly in a Delta table. In the following cells we run two separate **INSERT INTO** statements. Each of these is guaranteed to be an ACID transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES(1, 'Din', 'Djarik', 20, 20000);
# MAGIC INSERT INTO employees VALUES(2, 'Shane', 'Butler', 65, 50000);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The **UPDATE**, **SET** and **WHERE** statements allows us to perform a snapshot read of the latest version of a Delta table, find all fields that match our **WHERE** clause, and then apply the changes as described. In the following cells, we find all employees that have an age greater than 60, and add 10000 to their salary.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees 
# MAGIC SET salary = salary + 10000
# MAGIC WHERE age > 60;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Finally, the **DELETE** statement allows us atomically remove records from a target table. As such, there is no risk of only partially succeeding when removing data from your data lakehouse. Even when multiple records are deleted, it is always considered as a single transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Upserts in Delta tables
# MAGIC 
# MAGIC **MERGE**, in Databricks, allows for upserting records in a table, or loading records incrementally. Updates, Inserts, and other data manipulations can be run as part of a single upsert, or, in a single ACID transaction.
# MAGIC 
# MAGIC **MERGE** must have at least one field, generally a primary key, to match between a source, and a target table. Each **WHEN MATCHED** or **WHEN NOT MATCHED** clause can have any number of additional conditional statements.
# MAGIC 
# MAGIC In the following example, only 1 records gets loaded from the staging_employees table to employees table. In addition, only 1 records gets updated with new values.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES(1, 'Din', 'Djarik', 20, 20000);
# MAGIC INSERT INTO employees VALUES(2, 'Shane', 'Butler', 65, 50000);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS staging_employees
# MAGIC (id INT, name STRING, surname STRING, age INT, salary DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM staging_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO staging_employees VALUES(1, 'Din', 'Djarik', 52, 20000);
# MAGIC INSERT INTO staging_employees VALUES(2, 'Shane', 'Butler', 65, 50000);
# MAGIC INSERT INTO staging_employees VALUES(3, 'Nicola', 'Filosi', 42, 35000);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM staging_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employees AS b
# MAGIC USING staging_employees AS u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS staging_employees;
# MAGIC DROP TABLE IF EXISTS employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we introduced some basic SQL operations we can perform on Delta tables. We started with creating and deleting a Delta table, and with querying a Delta table. We then got familiar with operations such as inserting, updating, and deleting records. Finally, we covered a more advanced concept related to upserting and loading records incrementally.
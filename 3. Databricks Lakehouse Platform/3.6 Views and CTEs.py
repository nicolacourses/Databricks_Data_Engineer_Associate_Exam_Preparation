# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Views and CTEs on Databricks
# MAGIC In this video, we will create and manage views and common table expressions (CTEs) in Databricks.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Run Views SparkSQL DDL statements
# MAGIC - Run Common Table Expressions (CTEs) queries
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Database objects in Databricks</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run Views SparkSQL DDL statements
# MAGIC 
# MAGIC In Databricks, there are three main views types:
# MAGIC - (Stored) Views
# MAGIC - Temporary Views
# MAGIC - Global Temporary Views
# MAGIC 
# MAGIC As you will see in the cells here below, the syntax is very similar. It is important to remember though that:
# MAGIC - (Stored) Views are persisted as a database object
# MAGIC - Temporary Views exist only for the duration of a Spark Session
# MAGIC - Global Temporary Views exist only for the duration of a Spark cluster and are stored in the **global_temp db**.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE default;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vw_people
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = 'dbfs:/FileStore/people.csv',
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM temp_vw_people;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE people AS
# MAGIC SELECT * FROM temp_vw_people;
# MAGIC 
# MAGIC SELECT * FROM people;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_middle_aged_people AS
# MAGIC   SELECT * 
# MAGIC   FROM people 
# MAGIC   WHERE age >= 40;
# MAGIC 
# MAGIC SELECT * FROM vw_middle_aged_people;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW gl_vw_young_people;
# MAGIC AS SELECT * FROM people WHERE age < 30;
# MAGIC 
# MAGIC SELECT * FROM global_temp.vw_young_people;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's now demonstrate how temporary views, and global temporary views, last only for the duration of a Spark session or a duration of a cluster, respectively.
# MAGIC 
# MAGIC To do so, we can:
# MAGIC - Switch to a new notebook and try to reference a temporary view started in this very notebook
# MAGIC - Switch to a new notebook and try to reference a global temporary view
# MAGIC - Delete the cluster, start a new cluster, and try to reference a global temporary view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run Common Table Expressions (CTEs) queries
# MAGIC As discussed, a CTE (Common Table Expression) is simply a logical result of a query.
# MAGIC 
# MAGIC Here below we have a few examples of queries that incorporate CTEs which we can run:
# MAGIC - Aliasing multiple columns
# MAGIC - Multiple CTEs
# MAGIC - CTEs and subqueries
# MAGIC - CTEs and views

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH young_people AS
# MAGIC (
# MAGIC SELECT name AS young_name,
# MAGIC        surname AS young_surname,
# MAGIC        age AS young_age
# MAGIC FROM global_temp.vw_young_people
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM young_people AS YP
# MAGIC WHERE young_name = "Isabella";

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH young_people AS
# MAGIC (
# MAGIC SELECT name AS young_name,
# MAGIC        surname AS young_surname,
# MAGIC        age AS young_age
# MAGIC FROM global_temp.vw_young_people
# MAGIC ),
# MAGIC older_people AS
# MAGIC (
# MAGIC SELECT name AS middle_name,
# MAGIC        surname AS middle_surname,
# MAGIC        age AS middle_age
# MAGIC FROM vw_middle_aged_people
# MAGIC ),
# MAGIC total_people AS
# MAGIC (
# MAGIC SELECT * FROM young_people
# MAGIC UNION ALL
# MAGIC SELECT * FROM older_people
# MAGIC )
# MAGIC SELECT COUNT(*) FROM total_people;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM
# MAGIC (
# MAGIC   WITH young_selected_people AS
# MAGIC   (
# MAGIC   SELECT *
# MAGIC   FROM global_temp.vw_young_people
# MAGIC   WHERE id <= 3
# MAGIC   )
# MAGIC   SELECT *
# MAGIC   FROM young_selected_people
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW people_selection
# MAGIC AS
# MAGIC   WITH origin_people_selection AS
# MAGIC   (
# MAGIC   SELECT id,
# MAGIC          name,
# MAGIC          surname,
# MAGIC          age
# MAGIC   FROM vw_middle_aged_people
# MAGIC   WHERE id > 2 AND age < 50
# MAGIC   )
# MAGIC   SELECT *
# MAGIC   FROM origin_people_selection;
# MAGIC   
# MAGIC SELECT * FROM people_selection;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS people;
# MAGIC DROP VIEW IF EXISTS global_temp.vw_young_people;
# MAGIC DROP VIEW IF EXISTS people_selection;
# MAGIC DROP VIEW IF EXISTS vw_middle_aged_people;
# MAGIC DROP VIEW IF EXISTS temp_vw_people;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we worked primarily with views. We introduced all types of views with their respective syntax, and demonstrated what is their respective scope. In addition, we introduce the concept of the global_temp db. Finally, we had a look at some applications of Common Table Expressions (CTEs).
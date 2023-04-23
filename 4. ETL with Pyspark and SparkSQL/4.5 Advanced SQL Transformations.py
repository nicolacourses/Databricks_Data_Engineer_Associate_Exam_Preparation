# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Advanced SQL transformations
# MAGIC 
# MAGIC In this video, we will introduce a number of useful SparkSQL functions to tackle situations where we have to deal with dirty data, irregular data structures, complex queries with multiple objects, and so on.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Query nested data structures with the **.** syntax
# MAGIC - Work with JSON files, arrays, and structs
# MAGIC - Combine datasets with joins and set operations
# MAGIC - Reshape data with pivot statements
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/functions/schema_of_json.html" target="_blank">schema_of_json function</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/functions/from_json.html" target="_blank">from_json function</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/functions/explode.html" target="_blank">explode function</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/functions/collect_set.html" target="_blank">collect_set function</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/functions/array_distinct.html" target="_blank">array_distinct function</a>
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/functions/flatten.html" target="_blank">flatten function</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Interacting with JSON Data
# MAGIC 
# MAGIC SparkSQL supports several ways to interact with JSON data. In particular, we can:
# MAGIC - Parse JSON objects into struct types with the **from_json** function
# MAGIC - Use the **( . )** syntax to interact with JSON nested data structures
# MAGIC 
# MAGIC Let's expand a bit on the last point.
# MAGIC 
# MAGIC While the **from_json** function does require a schema, we can derive one via the **schema_of_json** function.
# MAGIC 
# MAGIC In the cell here below, we use both to first derive the schema we need, and then to cast the JSON **value** field to a struct type. We can then flatten the struct type via a **SELECT** statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_planets
# MAGIC   (planet_id INT,
# MAGIC    planet_age DECIMAL,
# MAGIC    planet_name STRING,
# MAGIC    atmosphere STRING,
# MAGIC    discovered STRING
# MAGIC    )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   path = "dbfs:/FileStore/planets.csv",
# MAGIC   delimiter = "|"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE vw_temp_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT atmosphere
# MAGIC FROM vw_temp_planets
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_temp_atmosphere_struct AS
# MAGIC   SELECT planet_id, planet_name,
# MAGIC   from_json(atmosphere, schema_of_json('{"exist":"Yes","pure":"No","polluted":"Yes","acid":"No","volcanic":"No"}')) 
# MAGIC   AS atmosphere_struct
# MAGIC   FROM vw_temp_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_atmosphere_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE vw_temp_atmosphere_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC        planet_id,
# MAGIC        planet_name,
# MAGIC        atmosphere_struct.acid,
# MAGIC        atmosphere_struct.exist,
# MAGIC        atmosphere_struct.polluted,
# MAGIC        atmosphere_struct.volcanic
# MAGIC FROM vw_temp_atmosphere_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC        planet_id,
# MAGIC        planet_name,
# MAGIC        atmosphere_struct.acid,
# MAGIC        atmosphere_struct.exist,
# MAGIC        atmosphere_struct.polluted,
# MAGIC        atmosphere_struct.volcanic
# MAGIC FROM vw_temp_atmosphere_struct
# MAGIC WHERE atmosphere_struct.acid = "Yes";

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Explore Data Structures
# MAGIC 
# MAGIC SparkSQL is well equipped to work with complex or irregular data structured. In particular, we can:
# MAGIC - Explode arrays with the **explode** function
# MAGIC - Fetch unique values from a field with the **collect_set** function
# MAGIC - Combine multiple arrays into a single array with the **flatten** function
# MAGIC - Remove duplicates from an array with the **array_distinct** function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT discovered
# MAGIC FROM vw_temp_planets
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_temp_discovered_struct AS
# MAGIC   SELECT planet_id, planet_name,
# MAGIC   from_json(discovered, schema_of_json('[{"explorer_name":"Mariner","explorer_surname":"Venier","explorer_institute":"NASA"},{"explorer_name":"Simone","explorer_surname":"Amadio","explorer_institute":"NASA"}]')) 
# MAGIC   AS discovered_struct
# MAGIC   FROM vw_temp_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT planet_id, planet_name, discovered_struct
# MAGIC FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT planet_id, planet_name, explode(discovered_struct)
# MAGIC FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT planet_id, planet_name, collect_set(discovered_struct.explorer_institute) AS explorer_institutes
# MAGIC FROM vw_temp_discovered_struct
# MAGIC GROUP BY planet_id, planet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_temp_explorer_institutes AS
# MAGIC SELECT planet_id, planet_name,
# MAGIC        collect_set(discovered_struct.explorer_institute) AS explorer_institutes,
# MAGIC        array_distinct(flatten(collect_set(discovered_struct.explorer_institute))) AS unique_explorer_institutes,
# MAGIC        explode(array_distinct(flatten(collect_set(discovered_struct.explorer_institute)))) AS final_explorer_institutes
# MAGIC FROM vw_temp_discovered_struct
# MAGIC GROUP BY planet_id, planet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_explorer_institutes
# MAGIC ORDER BY planet_id ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## JOINS and SET OPERATORS
# MAGIC 
# MAGIC SparkSQL supports:
# MAGIC - All traditional join operations (inner, outer, left, right, anti, cross, semi)
# MAGIC - The **UNION**, **INTERSECT** and **MINUS** set operators

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_all_planets AS
# MAGIC SELECT
# MAGIC       DIS.planet_id,
# MAGIC       DIS.planet_name,
# MAGIC       ATM.atmosphere_struct.acid,
# MAGIC       ATM.atmosphere_struct.exist,
# MAGIC       ATM.atmosphere_struct.polluted,
# MAGIC       ATM.atmosphere_struct.volcanic,
# MAGIC       DIS.final_explorer_institutes
# MAGIC FROM vw_temp_atmosphere_struct AS ATM
# MAGIC      LEFT JOIN vw_temp_explorer_institutes AS DIS
# MAGIC      ON ATM.planet_id = DIS.planet_id
# MAGIC ORDER BY planet_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vw_temp_all_planets
# MAGIC ORDER BY planet_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_acid_planets AS
# MAGIC WITH acid_atmosphere AS
# MAGIC (
# MAGIC SELECT 
# MAGIC        planet_id,
# MAGIC        planet_name,
# MAGIC        atmosphere_struct.acid,
# MAGIC        atmosphere_struct.exist,
# MAGIC        atmosphere_struct.polluted,
# MAGIC        atmosphere_struct.volcanic
# MAGIC FROM vw_temp_atmosphere_struct
# MAGIC WHERE atmosphere_struct.acid = "Yes"
# MAGIC )
# MAGIC SELECT
# MAGIC       DIS.planet_id,
# MAGIC       DIS.planet_name,
# MAGIC       ATM.acid,
# MAGIC       ATM.exist,
# MAGIC       ATM.polluted,
# MAGIC       ATM.volcanic,
# MAGIC       DIS.final_explorer_institutes
# MAGIC FROM acid_atmosphere AS ATM
# MAGIC      INNER JOIN vw_temp_explorer_institutes AS DIS
# MAGIC      ON ATM.planet_id = DIS.planet_id
# MAGIC ORDER BY planet_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_acid_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_acid_planets 
# MAGIC UNION ALL
# MAGIC SELECT * FROM vw_temp_all_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_acid_planets
# MAGIC UNION
# MAGIC SELECT * FROM vw_temp_all_planets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_temp_acid_planets
# MAGIC INTERSECT
# MAGIC SELECT * FROM vw_temp_all_planets;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## PIVOT Tables
# MAGIC 
# MAGIC The **PIVOT** statement is useful to reshape data and calculate new aggregations. Its syntax works as follows:
# MAGIC - **SELECT * FROM ()** defines the input dataset for the **PIVOT** statement
# MAGIC - **PIVOT**: reshapes the data. In particular, the first argument its an aggregate function, and its target column. The second argument, **FOR**, defines the column that will be pivoted. Finally, the third argument, **IN**, includes the pivot column values.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pivoted_explorer_institutes AS
# MAGIC 
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC     planet_id,
# MAGIC     planet_name,
# MAGIC     final_explorer_institutes
# MAGIC   FROM vw_temp_all_planets
# MAGIC ) PIVOT (
# MAGIC   COUNT(final_explorer_institutes) FOR final_explorer_institutes in (
# MAGIC     'NASA',
# MAGIC     'NA',
# MAGIC     'Soviet Union',
# MAGIC     'URSS'
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM pivoted_explorer_institutes
# MAGIC ORDER BY planet_id ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we started with a raw .csv file which included JSON objects and we explored the . syntax to interact with them. We then went ahead and got familiar with a set of functions that allow us to work with more complex data structures, such as an attribute, with nested JSON objects, inside of an array. Finally, we had a look at some more traditional, but still davanced, SQL operations such as joins, set operators, and the pivot statement.
# MAGIC 
# MAGIC As a note, while we did work with quite a few views, please remember they are all temporary views. As such, they will cease to exist as soon as we exit this notebook.
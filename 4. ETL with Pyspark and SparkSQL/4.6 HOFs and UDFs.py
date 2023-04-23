# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Higher Order Functions (HOFs) and User Defined Functions (UDFs)
# MAGIC In this video, we will get familiar with SparkSQL higher order functions. They are especially useful when working with complex data types, such as array or map type objects. With higher-order functions, we can alter data stored in those formats, while maintaining the original structure.
# MAGIC 
# MAGIC Higher order functions include:
# MAGIC - **FILTER** filters an array using a given lambda function.
# MAGIC - **TRANSFORM** uses a given lambda function to transform all elements in an array.
# MAGIC 
# MAGIC In addition, we will understand how can we define and register our own SparkSQL user defined functions. With this feature, we can encapsulate custom SQL logic and store it as a function object in a database. 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Define and registering SQL UDFs
# MAGIC - Describe the security model used for sharing SQL UDFs
# MAGIC - Use **`CASE`** / **`WHEN`** statements in SQL code
# MAGIC - Leverage **`CASE`** / **`WHEN`** statements in SQL UDFs for custom control flow
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/optimizations/higher-order-lambda-functions.html#introduction-to-higher-order-functions-notebook" target="_blank">Higher Order Functions</a>
# MAGIC - <a href="https://www.databricks.com/blog/2021/10/20/introducing-sql-user-defined-functions.html" target="_blank">SparkSQL User Defined Functions (UDFs)</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Filter
# MAGIC 
# MAGIC The **Filter** function syntax is: **FILTER (items, i -> i.item_id LIKE "%K") AS king_items**. In particular:
# MAGIC - **FILTER** : the name of the higher-order function <br>
# MAGIC - **discovered_struct** : the name of our input array <br>
# MAGIC - **i** : the name of the iterator variable. We choose this name to use it in the lambda function. It iterates over each value of the input array, one at a time.<br>
# MAGIC - **->** :  Indicates the start of a function <br>
# MAGIC - **i.explorer_institute = "NASA"** : this is the actual function. It checks each value in the input array, in the explorer_institute attribute, to see whether they match to **NASA**. If yes, they get filtered to the new column **selected_institute**.

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
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW vw_temp_discovered_struct AS
# MAGIC   SELECT planet_id, planet_name, planet_age,
# MAGIC   from_json(discovered, schema_of_json('[{"explorer_name":"Mariner","explorer_surname":"Venier","explorer_institute":"NASA"},{"explorer_name":"Simone","explorer_surname":"Amadio","explorer_institute":"NASA"}]')) 
# MAGIC   AS discovered_struct
# MAGIC   FROM vw_temp_planets;
# MAGIC 
# MAGIC SELECT * FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   planet_id,
# MAGIC   planet_name,
# MAGIC   discovered_struct,
# MAGIC   FILTER (discovered_struct, i -> i.explorer_institute = "NASA") AS selected_institute
# MAGIC FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH selected_institute AS
# MAGIC (
# MAGIC SELECT
# MAGIC   planet_id,
# MAGIC   planet_name,
# MAGIC   discovered_struct,
# MAGIC   FILTER (discovered_struct, i -> i.explorer_institute = "NASA") AS selected_institute
# MAGIC FROM vw_temp_discovered_struct
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM selected_institute
# MAGIC WHERE size(selected_institute) > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transform
# MAGIC 
# MAGIC The **Transform** function syntax is: **TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues.**
# MAGIC 
# MAGIC In particular:
# MAGIC - **discovered_struct** is the input array
# MAGIC - **k** is the iterator variable that iterates over the array, one value at a time
# MAGIC - **k.explorer_name, k.explorer_surname** are the attributes to which we are applying a function
# MAGIC - **UPPER** is another function which we apply to our target attributes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   planet_id,
# MAGIC   planet_name,
# MAGIC   TRANSFORM (discovered_struct, k -> UPPER(k.explorer_name)) AS uppercase_explorer_name,
# MAGIC   TRANSFORM (discovered_struct, k -> UPPER(k.explorer_surname)) AS uppercase_explorer_surname
# MAGIC FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SparkSQL UDFs
# MAGIC To create a SparkSQL UDF, we need a few fundamental elements:
# MAGIC - A function name
# MAGIC - The type to be returned
# MAGIC - The logic incapsulated in the function
# MAGIC 
# MAGIC A SparkSQL UDF is applied to all values of a target column in a parallel fashion within the Spark processing engine. SQL UDFs are an efficient way to define custom logic that is optimized for execution on Databricks.
# MAGIC 
# MAGIC Another interesting use case for a UDF is using the CASE/WHEN statements to apply flow control in SQL.
# MAGIC 
# MAGIC It is important to note that a UDF will persist between execution environments, as they are stored in a database. We can describe a UDF to return useful information.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION convert_age(planet_age INT)
# MAGIC RETURNS INT
# MAGIC RETURN (planet_age / 1.14);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT planet_age, convert_age(planet_age) AS converted_age FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED convert_age;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION suitable_planets(planet_name STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC   WHEN planet_name = "Mercury" THEN "Life can thrive here"
# MAGIC   WHEN planet_name = "Proxima Centaury" THEN "Life can thrive here"
# MAGIC   ELSE "Life cannot thrive here"
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT planet_name, suitable_planets(planet_name) AS planet_suitability_check FROM vw_temp_discovered_struct;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS convert_age;
# MAGIC DROP FUNCTION IF EXISTS suitable_planets;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we first of all introduced Higher Order Functions. These are advanced functions with which we can manipulate data inside of complex data structures such as arrays, without reshaping the data structure itself. We then covered SparkSQL User Defined Functions: a convenient and efficient way to incapsulate a piece of code in a database function, to reuse it over and over again over a view, a table, or in a query.
# MAGIC 
# MAGIC We did use quite a few views in this video. Still, as they are all temporary view, they will cease to exist as soon as we exit this notebook.
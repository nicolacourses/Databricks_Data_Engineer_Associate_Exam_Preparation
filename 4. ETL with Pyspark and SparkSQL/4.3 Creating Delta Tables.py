# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Creating Delta Tables
# MAGIC In this video we will focus on the process of creating tables utilizing **CREATE TABLE AS SELECT** (CTAS) statements.
# MAGIC 
# MAGIC As we might have inferred in the previous video, once data has been extracted from external data sources, it is recommended to load the data into the Lakehouse, so that all the Delta Lake advantages can be exploited to the fullest extent.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Create Delta tables with CTAS statements
# MAGIC - Create tables from existing database objects
# MAGIC - Enrich data with additional metadata
# MAGIC - Declare table schemas with new columns and descriptive comments
# MAGIC - Set options to manage data location, quality enforcement, and partitioning
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">Create Table in Databricks - Details</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Delta tables with CTAS statements
# MAGIC 
# MAGIC The **CREATE TABLE AS SELECT** statement creates and fills Delta tables by retrieving data from an input query. In particular, it:
# MAGIC - Automatically deduces schema information from query results and *do not allow* manual schema declarations
# MAGIC - Is excellent for importing data from external sources with established schemas, such as Parquet files
# MAGIC - Does not support specifying supplementary file options
# MAGIC 
# MAGIC Creating a table with a CTAS statement while fetching data from .csv files can pose significant obstacles. As a work around, we will need to create a reference database object to the .csv files. Such object will help us to specifying those supplementary file options. This is similar to what we did as part of our demonstration in the previous video.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_parquet AS
# MAGIC SELECT * FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_csv AS
# MAGIC SELECT * FROM csv.`dbfs:/FileStore/sales.csv`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sales_tmp_vw
# MAGIC (
# MAGIC Date DATE,
# MAGIC Product STRING,
# MAGIC Category STRING,
# MAGIC Price DOUBLE,
# MAGIC Quantity INT,
# MAGIC Sales DOUBLE
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "dbfs:/FileStore/sales.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_delta_table AS
# MAGIC   SELECT * FROM sales_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_delta_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Declare Schema with Generated Columns
# MAGIC We already mentioned that CTAS statements do not support schema declaration. Because of this, we might sometimes need to rely on generated columns.
# MAGIC 
# MAGIC Generated columns are a distinctive category of column whose values are automatically produced through a user-specified function that is based on other columns in the Delta table.
# MAGIC 
# MAGIC The cell below creates a new table and:
# MAGIC - Declares column names and data types
# MAGIC - Adds a generated column to calculate dates
# MAGIC - Provides a descriptive comment for the generated column

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_enriched
# MAGIC (
# MAGIC Date DATE,
# MAGIC Product STRING,
# MAGIC Category STRING,
# MAGIC Price DOUBLE,
# MAGIC Quantity INT,
# MAGIC Sales DOUBLE,
# MAGIC Adjusted_Sales INT GENERATED ALWAYS AS (
# MAGIC CAST(Sales/1.14 AS INT))
# MAGIC COMMENT "generated based on `transactions_timestamp` column"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As the **date** column is generated column, if we write to **sales_dates** without supplying values for the **date** column, Delta Lake will automatically compute those values.
# MAGIC 
# MAGIC The next cell inserts records incrementally with **MERGE** in the table with the generated column. It includes a setting to allow for that generated columns, and for generating columns in general, while using **MERGE**. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true;
# MAGIC 
# MAGIC MERGE INTO sales_enriched AS a
# MAGIC USING sales_delta_table AS b
# MAGIC ON a.Date = b.Date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Table Constraints
# MAGIC 
# MAGIC Databricks currently support two types of constraints:
# MAGIC - NOT NULL constraints
# MAGIC - CHECK constraints
# MAGIC 
# MAGIC It is critical to confirm that no data infringing on the constraint exists in the table prior to defining the constraints in any case. When a constraint is applied to a table, any data contravening the constraint will result in the failure of a write operation.
# MAGIC 
# MAGIC In the cell here below, we will add a **CHECK** constraint to the **date** column.
# MAGIC 
# MAGIC Table constraints are shown in the **TBLPROPERTIES** field.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sales_enriched ADD CONSTRAINT valid_date CHECK (date >= '2022-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Enrich Tables with Additional Options and Metadata
# MAGIC 
# MAGIC We have the opportunity to add useful metadata to our Delta tables using CTAS statements.
# MAGIC 
# MAGIC In the cell here below, we:
# MAGIC - Record the query execution datetime with **current_timestamp()**
# MAGIC - Record the source data file per each and every record with **input_file_name()**
# MAGIC - Include a **COMMENT** to better describe the table's data
# MAGIC - Include a **LOCATION** to define an external (unmanaged) table, rather than a managed table
# MAGIC - Partition with **PARTITION BY** on the date column, meaning that data from each data will exist in its own directory in the specified location.
# MAGIC 
# MAGIC It is important to note that generally speaking we should avoid using the **PARTITION BY** argument.
# MAGIC 
# MAGIC Most Delta Lake tables (especially those with small-to-medium data volumes) do not benefit from partitioning. As partitioning physically separates data files, this method can result in a small files issue, which can obstruct file compaction and efficient data skipping.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_augmented
# MAGIC COMMENT "Contains additional useful metadata"
# MAGIC LOCATION "dbfs:/tables_from_parquet/"
# MAGIC PARTITIONED BY (Date)
# MAGIC AS
# MAGIC   SELECT *, 
# MAGIC          current_timestamp() AS insert_timestamp,
# MAGIC          input_file_name() source_file
# MAGIC   FROM parquet.`dbfs:/FileStore/sales.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_augmented;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_augmented;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Cloning Delta Lake Tables
# MAGIC Delta Lake can copy Delta tables in two ways:
# MAGIC - **DEEP CLONE** completely duplicates the data and metadata from a source table to a target. This duplication occurs incrementally, so re-executing this command can synchronize any changes made to the source location with the target. For larger datasets, this can take a while
# MAGIC - **SHALLOW CLONE** copies a table much more quickly. It clones only the Delta transaction logs, meaning that the data files are left untouched
# MAGIC 
# MAGIC Regardless of the method used, any changes made to the cloned table will be recorded and kept distinct from the source data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_augmented_deep_clone
# MAGIC DEEP CLONE sales_augmented;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_augmented_deep_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_augmented_shallow_clone
# MAGIC SHALLOW CLONE sales_augmented;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_augmented_shallow_clone;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

dbutils.fs.rm("dbfs:/tables_from_parquet", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_agumented;
# MAGIC DROP TABLE IF EXISTS sales_csv;
# MAGIC DROP TABLE IF EXISTS sales_dates;
# MAGIC DROP TABLE IF EXISTS sales_delta;
# MAGIC DROP TABLE IF EXISTS sales_delta_table;
# MAGIC DROP TABLE IF EXISTS sales_enriched;
# MAGIC DROP TABLE IF EXISTS sales_parquet;
# MAGIC DROP TABLE IF EXISTS sales_augmented;
# MAGIC DROP TABLE IF EXISTS sales_augmented_shallow_clone;
# MAGIC DROP TABLE IF EXISTS sales_augmented_deep_clone;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC In this video, we first of all explored the Create Table As Select (CTAS) statement. We had a look at using it both with well formed data formats, such as parquet, and less suitable data formats, such as csv. Regarding this latter case, we created a view to ingest the raw data from the CSV file, and then we used a CTAS statement to created our table. We then proceeed by introducing constraints on Delta tables and by suggesting useful metatada with which augmenting raw data from files. Finally, we touched on copying tables either via deep clones, or shallow clones.
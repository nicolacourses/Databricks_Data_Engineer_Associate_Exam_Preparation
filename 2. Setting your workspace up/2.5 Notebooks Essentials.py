# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Notebooks Essentials
# MAGIC In this video, we will introduce the fundamental concepts of working with Databricks notebooks and delve into the additional features that Databricks Repos introduces to notebooks. Databricks notebooks are the primary mode to develop and execute code.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC Once completed this video, we should be able to:
# MAGIC - Attach a notebook to a cluster
# MAGIC - Set the language for a notebook
# MAGIC - Use magic commands and run notebook cells
# MAGIC - Create a markdown notebook cell
# MAGIC - Run a notebook from another notebook
# MAGIC - Use Databricks Utilities
# MAGIC - Import and Export one or more Notebooks
# MAGIC - Use basic Notebooks Version Control
# MAGIC 
# MAGIC ## Documentation References
# MAGIC - <a href="https://docs.databricks.com/notebooks/index.html" target="_blank">Introduction to Databricks Notebooks</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Attach a Notebook to a Cluster
# MAGIC 
# MAGIC Located at the top right of your screen, there is a dropdown menu that you can use to establish a connection between this notebook and your cluster of choice.
# MAGIC 
# MAGIC Attaching a notebook to a cluster can take a few minutes. Once the cluster has been deployed, a green arrow will appear to the right of the cluster name.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run a Notebook Cell
# MAGIC 
# MAGIC To run a cell in a notebook, we can use one of the following options:
# MAGIC - **CTRL+ENTER** or **CTRL+RETURN**
# MAGIC - **SHIFT+ENTER** or **SHIFT+RETURN** to run a specific notebook cell and move to the next one
# MAGIC - **Run Cell**, **Run All Above** or **Run All Below**
# MAGIC 
# MAGIC Isolating our code in notebooks cells allows for cells to be run multiple times or in a non-sequential order.
# MAGIC 
# MAGIC In this course, we should always execute notebooks cells one at a time starting from the top and working downards. While using notebooks, common errors can often be resolved by either executing earlier cells that were not run or by restarting the notebook from the beginning.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set the Language for a Notebook
# MAGIC 
# MAGIC Databricks notebooks can support Python, SQL, Scala, and R. A language can be selected when a notebook is created, and changed at any time. A notebook's default language appears directly to the right of the notebook title at the top of the page.
# MAGIC 
# MAGIC To change the default language for a notebook:
# MAGIC - Click on **Python**, next to the notebook title, in the top left corner of your screen
# MAGIC - Select your language of choice (for example, **SQL**) from the drop down menu

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use Magic Commands
# MAGIC In Databricks notebooks, there are specific commands known as magic commands.
# MAGIC 
# MAGIC These commands are integrated into the system and can be used in any notebook language to produce consistent outcomes. To identify a magic command, it must be preceded by a single percent (%) symbol at the beginning of a cell. It is important to note that only one magic command can be used per cell and that it must be the first command in the cell.
# MAGIC 
# MAGIC Examples of **Magic Commands** are %python or %sql.
# MAGIC 
# MAGIC A best practice is choosing a primary language for a notebook, and then using the magic commands to execute code in another language only when needed, in specific cells.

# COMMAND ----------

# MAGIC %python
# MAGIC print("Hello World!");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello World!"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a Markdown Notebook Cell
# MAGIC 
# MAGIC The magic command **%md** allows us to format text differently in a notebook cell.
# MAGIC 
# MAGIC A few examples are:
# MAGIC 
# MAGIC # Example One
# MAGIC ## Example Two
# MAGIC ### Example Three
# MAGIC 
# MAGIC **bold**
# MAGIC *italicized*
# MAGIC 
# MAGIC Ordered list
# MAGIC 1. once
# MAGIC 2. two
# MAGIC 3. three
# MAGIC 
# MAGIC Unordered list
# MAGIC - region
# MAGIC - country
# MAGIC - city
# MAGIC 
# MAGIC Links/Embedded HTML: <a href="https://www.databricks.com/" target="_blank">Databricks</a>
# MAGIC 
# MAGIC Images:
# MAGIC ![Spark Engines](https://www.databricks.com/wp-content/uploads/2020/04/og-databricks.png)
# MAGIC 
# MAGIC Tables
# MAGIC 
# MAGIC | id     | age |
# MAGIC |--------|-----|
# MAGIC | 1      | 10  |
# MAGIC | 2      | 20  |
# MAGIC | 3      | 34  |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run a notebook from another notebook
# MAGIC 
# MAGIC To execute a notebook from within another notebook, we can use the magic command %run, and add a path.
# MAGIC 
# MAGIC The noteboook we reference will run as if it were a part of the current notebook. This means, for instance, that local declarations and temporary views will be accessible from the calling notebook.

# COMMAND ----------

# MAGIC %run "/Users/user@company.com/User/Notebook_Name"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Databricks Utilities
# MAGIC 
# MAGIC Databricks notebooks provide a number of utility commands for configuring and interacting with the environment:
# MAGIC 
# MAGIC - **dbutils.help()** lists all Databricks utilities commands
# MAGIC - **dbutils.fs.help()** lists all Databricks file system utilities commands
# MAGIC - **dbutils.fs.ls()** returns a list of directories or files in a target path
# MAGIC - **display()** returns tabular SQL-like results when running Python code

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Import and export one or more Notebooks
# MAGIC In this vdeo, we will have a look at the steps required to download either this single notebook, and a collection of all notebooks contained in this course.
# MAGIC 
# MAGIC To export a single notebook:
# MAGIC - On the top left corner of your screen, click the **File** option
# MAGIC - Hover over **Export** and then select **Source File** from the drop down menu
# MAGIC 
# MAGIC The notebook will download to your local machine. Its name will be the actual notebook name, and its extension will be the notebook default language. You can open the downloaded notebook with any file editor and see the raw contents of Databricks notebooks.
# MAGIC 
# MAGIC To export more than one notebook:
# MAGIC - Hover over the **Repos** page on the left sidebar
# MAGIC - Look for a directory called **Data Engineering with Databricks**
# MAGIC - Click the down arrow to bring up a menu
# MAGIC - Hover over **Export** and select **DBC Archive**
# MAGIC 
# MAGIC After downloading, the Databricks Cloud (DBC) file will contain a compressed archive of all the directories and notebooks found within this course. It is important to note that we should avoid editing these DBC files locally. Instead, we can safely upload them into any Databricks workspace to move or share notebook contents.
# MAGIC 
# MAGIC To import one or more notebooks:
# MAGIC - On the top left corner of your screen, click the **File** option
# MAGIC - Hover over **Import** and then select either **File** or **URL** from the drop down menu

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Notebooks Version Control
# MAGIC 
# MAGIC Databricks Notebooks come with some basic Version Control features that allow for time-traveling and version restoring.
# MAGIC 
# MAGIC To access such features:
# MAGIC - Click on **last edit was x minutes ago** right below the Notebook title
# MAGIC - On the right of your screen, scroll through the different versions and changes of your notebook
# MAGIC 
# MAGIC Each tab represents a previous version of your notebook. It is possible to restore the notebook to a previous version by clicking **Restore Now** in the tab of choice.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC In this video, we introduced Databricks notebooks as the primary mean through which we develop and execute code in Databricks. We covered both the basics, such as attaching a notebook to a cluster, running a notebook cell, and using magic commands; and more advanced topics, for example using Databricks utilities, exporting and importing notebooks, and notebooks version control.
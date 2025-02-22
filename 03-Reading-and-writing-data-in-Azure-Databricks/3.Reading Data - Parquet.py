# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data - Parquet Files
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC - Introduce the Parquet file format.
# MAGIC - Read data from:
# MAGIC   - Parquet files without a schema.
# MAGIC   - Parquet files with a schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# Mount "/mnt/training" again using "%run "./Includes/Dataset-Mounts-New"" if it is failed in "./Includes/Classroom-Setup"
try:
    files = dbutils.fs.ls("/mnt/training")
except:
    dbutils.fs.unmount('/mnt/training/')


# COMMAND ----------

# MAGIC %run "./Includes/Dataset-Mounts-New"

# COMMAND ----------

# MAGIC %run "./Includes/Utility-Methods"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="float:right; margin-right:1em">
# MAGIC   <img src="https://parquet.apache.org/assets/img/parquet_logo.png"><br>
# MAGIC   <a href="https://parquet.apache.org/" target="_blank">https&#58;//parquet.apache.org</a>
# MAGIC </div>
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from Parquet Files
# MAGIC
# MAGIC <strong style="font-size:larger">"</strong>Apache Parquet is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.<strong style="font-size:larger">"</strong><br>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### About Parquet Files
# MAGIC * Free & Open Source.
# MAGIC * Increased query performance over row-based data stores.
# MAGIC * Provides efficient data compression.
# MAGIC * Designed for performance on large data sets.
# MAGIC * Supports limited schema evolution.
# MAGIC * Is a splittable "file format".
# MAGIC * A <a href="https://en.wikipedia.org/wiki/Column-oriented_DBMS" target="_blank">Column-Oriented</a> data store
# MAGIC
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;** Row Format ** &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **Column Format**
# MAGIC
# MAGIC <table style="border:0">
# MAGIC
# MAGIC   <tr>
# MAGIC     <th>ID</th><th>Name</th><th>Score</th>
# MAGIC     <th style="border-top:0;border-bottom:0">&nbsp;</th>
# MAGIC     <th>ID:</th><td>1</td><td>2</td>
# MAGIC     <td style="border-right: 1px solid #DDDDDD">3</td>
# MAGIC   </tr>
# MAGIC
# MAGIC   <tr>
# MAGIC     <td>1</td><td>john</td><td>4.1</td>
# MAGIC     <td style="border-top:0;border-bottom:0">&nbsp;</td>
# MAGIC     <th>Name:</th><td>john</td><td>mike</td>
# MAGIC     <td style="border-right: 1px solid #DDDDDD">sally</td>
# MAGIC   </tr>
# MAGIC
# MAGIC   <tr>
# MAGIC     <td>2</td><td>mike</td><td>3.5</td>
# MAGIC     <td style="border-top:0;border-bottom:0">&nbsp;</td>
# MAGIC     <th style="border-bottom: 1px solid #DDDDDD">Score:</th>
# MAGIC     <td style="border-bottom: 1px solid #DDDDDD">4.1</td>
# MAGIC     <td style="border-bottom: 1px solid #DDDDDD">3.5</td>
# MAGIC     <td style="border-bottom: 1px solid #DDDDDD; border-right: 1px solid #DDDDDD">6.4</td>
# MAGIC   </tr>
# MAGIC
# MAGIC   <tr>
# MAGIC     <td style="border-bottom: 1px solid #DDDDDD">3</td>
# MAGIC     <td style="border-bottom: 1px solid #DDDDDD">sally</td>
# MAGIC     <td style="border-bottom: 1px solid #DDDDDD; border-right: 1px solid #DDDDDD">6.4</td>
# MAGIC   </tr>
# MAGIC
# MAGIC </table>
# MAGIC
# MAGIC See also
# MAGIC * <a href="https://parquet.apache.org/" target="_blank">https&#58;//parquet.apache.org</a>
# MAGIC * <a href="https://en.wikipedia.org/wiki/Apache_Parquet" target="_blank">https&#58;//en.wikipedia.org/wiki/Apache_Parquet</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Source
# MAGIC
# MAGIC The data for this example shows the number of requests to Wikipedia's mobile and desktop websites (<a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">23 MB</a> from Wikipedia). 
# MAGIC
# MAGIC The original file, captured August 5th of 2016 was downloaded, converted to a Parquet file and made available for us at **/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/**

# COMMAND ----------

# MAGIC %fs ls /mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/

# COMMAND ----------

# MAGIC %md
# MAGIC Unlike our CSV and JSON example, the parquet "file" is actually 11 files, 8 of which consist of the bulk of the data and the other three consist of meta-data. 

# COMMAND ----------

# MAGIC %fs ls /mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/

# COMMAND ----------

# MAGIC %fs head "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/part-00000-2f6ae1e7-8431-4663-a275-3d2c5cc46e6e-c000.gz.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in the Parquet Files
# MAGIC
# MAGIC To read in this files, we will specify the location of the parquet directory.

# COMMAND ----------

parquetFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/"

(spark.read              # The DataFrameReader
  .parquet(parquetFile)  # Creates a DataFrame from Parquet after reading in the file
  .printSchema()         # Print the DataFrame's schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review: Reading from Parquet Files
# MAGIC * We do not need to specify the schema - the column names and data types are stored in the parquet files.
# MAGIC * Only one job is required to **read** that schema from the parquet file's metadata.
# MAGIC * Unlike the CSV or JSON readers that have to load the entire file and then infer the schema, the parquet reader can "read" the schema very quickly because it's reading that schema from the metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in the Parquet Files w/Schema
# MAGIC
# MAGIC If you want to avoid the extra job entirely, we can, again, specify the schema even for parquet files:
# MAGIC
# MAGIC ** *WARNING* ** *Providing a schema may avoid this one-time hit to determine the `DataFrame's` schema.*  
# MAGIC *However, if you specify the wrong schema it will conflict with the true schema and will result in an analysis exception at runtime.*

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

parquetSchema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

(spark.read               # The DataFrameReader
  .schema(parquetSchema)  # Use the specified schema
  .parquet(parquetFile)   # Creates a DataFrame from Parquet after reading in the file
  .printSchema()          # Print the DataFrame's schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

# COMMAND ----------

parquetDF = spark.read.schema(parquetSchema).parquet(parquetFile)

print("Partitions: " + str(parquetDF.rdd.getNumPartitions()) )
printRecordsPerPartition(parquetDF)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In most/many cases, people do not provide the schema for Parquet files because reading in the schema is such a cheap process.
# MAGIC
# MAGIC And lastly, let's peek at the data:

# COMMAND ----------

display(parquetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Start the next lesson, [Reading Data - Tables and Views]($./4.Reading%20Data%20-%20Tables%20and%20Views)

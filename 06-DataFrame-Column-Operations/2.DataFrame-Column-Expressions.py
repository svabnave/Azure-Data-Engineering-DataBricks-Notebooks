# Databricks notebook source
# MAGIC %md
# MAGIC #DataFrame Column Expressions
# MAGIC
# MAGIC ** Data Source **
# MAGIC * One hour of Pagecounts from the English Wikimedia projects captured August 5, 2016, at 12:00 PM UTC.
# MAGIC * Size on Disk: ~23 MB
# MAGIC * Type: Compressed Parquet File
# MAGIC * More Info: <a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">Page view statistics for Wikimedia projects</a>
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC * Continue exploring the `DataFrame` set of APIs.
# MAGIC * Continue to work with the `Column` class and introduce the `Row` class
# MAGIC * Introduce the transformations...
# MAGIC   * `orderBy(..)`
# MAGIC   * `sort(..)`
# MAGIC   * `filter(..)`
# MAGIC   * `where(..)`
# MAGIC * Introduce the actions...
# MAGIC   * `collect()`
# MAGIC   * `take(n)`
# MAGIC   * `first()`
# MAGIC   * `head()`

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

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **The Data Source**
# MAGIC
# MAGIC We will be using the same data source as our previous notebook.
# MAGIC
# MAGIC As such, we can go ahead and start by creating our initial `DataFrame`.

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)


source = '/mnt/training'
parquetFile = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetFile)     # Returns an instance of DataFrame
  .cache()                  # cache the data
)
print(pagecountsEnAllDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's look at the data once more...

# COMMAND ----------

from pyspark.sql.functions import *

sortedDescDF = (pagecountsEnAllDF
  .orderBy( col("requests").desc() )
)  
sortedDescDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In looking at the data, we can see multiple Wikipedia projects.
# MAGIC
# MAGIC What if we want to look at only the main Wikipedia project, **en**?
# MAGIC
# MAGIC For that, we will need to filter out some records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) filter(..) & where(..)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC If you look at the API docs, `filter(..)` and `where(..)` are described like this:
# MAGIC > Filters rows using the given condition.
# MAGIC
# MAGIC Both `filter(..)` and `where(..)` return a new dataset containing only those records for which the specified condition is true.
# MAGIC * Like `distinct()` and `dropDuplicates()`, `filter(..)` and `where(..)` are aliases for each other.
# MAGIC   * `filter(..)` appealing to functional programmers.
# MAGIC   * `where(..)` appealing to developers with an SQL background.
# MAGIC * Like `orderBy(..)` there are two variants of these two methods:
# MAGIC   * `filter(Column)`
# MAGIC   * `filter(String)`
# MAGIC   * `where(Column)`
# MAGIC   * `where(String)`
# MAGIC * Unlike `orderBy(String)` which requires a column name, `filter(String)` and `where(String)` both expect an SQL expression.
# MAGIC
# MAGIC Let's start by looking at the variant using an SQL expression:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### filter(..) & where(..) w/SQL Expression

# COMMAND ----------

whereDF = (sortedDescDF
  .where( "project = 'en'" )
)
whereDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we are only looking at the main Wikipedia articles, we get a better picture of the most popular articles on Wikipedia.
# MAGIC
# MAGIC Next, let's take a look at the second variant that takes a `Column` object as its first parameter:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### filter(..) & where(..) w/Column

# COMMAND ----------

filteredDF = (sortedDescDF
  .filter( col("project") == "en")
)
filteredDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Scala Issue...

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC With Python, this is pretty straight forward.
# MAGIC
# MAGIC But in Scala... notice anything unusual in that last command?
# MAGIC
# MAGIC **Question:** In most every programming language, what is a single equals sign (=) used for?
# MAGIC
# MAGIC **Question:** What are two equal signs (==) used for?
# MAGIC
# MAGIC **Question:** 
# MAGIC * Considering that transformations are lazy...
# MAGIC * And the == operator executes now...
# MAGIC * And `filter(..)` and `where(..)` require us to pass a `Column` object...
# MAGIC * What would be wrong with `$"project" == "en"`?
# MAGIC
# MAGIC Try it...

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC $"project" == "en"

# COMMAND ----------

# MAGIC %md
# MAGIC Compare that to the following call...

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC $"project" === "en"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the Scala Doc for the `Column` object. </br>
# MAGIC
# MAGIC | "Operator" | Function |
# MAGIC |:----------:| -------- |
# MAGIC | === | Equality test |
# MAGIC | !== | Deprecated inequality test |
# MAGIC | =!= | Inequality test |
# MAGIC | <=> | Null safe equality test |

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Solution...

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC With that behind us, we can clearly **see** the top ten most requested articles.
# MAGIC
# MAGIC But what if we need to **programmatically** extract the value of the most requested article's name and its number of requests?
# MAGIC
# MAGIC That is to say, how do we get the first record, and from there...
# MAGIC * the value of the second column, **article**, as a string...
# MAGIC * the value of the third column, **requests**, as an integer...
# MAGIC
# MAGIC Before we proceed, let's apply another filter to get rid of **Main_Page** and anything starting with **Special:** - they're just noise to us.

# COMMAND ----------

articlesDF = (filteredDF
  .drop("bytes_served")
  .filter( col("article") != "Main_Page")
  .filter( col("article") != "-")
  .filter( col("article").startswith("Special:") == False)
)
articlesDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) first() & head()
# MAGIC
# MAGIC If you look at the API docs, both `first(..)` and `head(..)` are described like this:
# MAGIC > Returns the first row.
# MAGIC
# MAGIC Just like `distinct()` & `dropDuplicates()` are aliases for each other, so are `first(..)` and `head(..)`.
# MAGIC
# MAGIC However, unlike `distinct()` & `dropDuplicates()` which are **transformations** `first(..)` and `head(..)` are **actions**.
# MAGIC
# MAGIC Once all processing is done, these methods return the object backing the first record.
# MAGIC
# MAGIC In the case of `DataFrames` (both Scala and Python) that object is a `Row`.
# MAGIC
# MAGIC In the case of `Datasets` (the strongly typed version of `DataFrames` in Scala and Java), the object may be a `Row`, a `String`, a `Customer`, a `PendingApplication` or any number of custom objects.
# MAGIC
# MAGIC Focusing strictly on the `DataFrame` API for now, let's take a look at a call with `head()`:

# COMMAND ----------

firstRow = articlesDF.first()

print(firstRow)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Row Class
# MAGIC
# MAGIC Now that we have a reference to the object backing the first row (or any row), we can use it to extract the data for each column.
# MAGIC
# MAGIC Before we do, let's take a look at the API docs for the `Row` class.
# MAGIC
# MAGIC At the heart of it, we are simply going to ask for the value of the object in column N via `Row.get(i)`.
# MAGIC
# MAGIC Python being a loosely typed language, the return value is of no real consequence.
# MAGIC
# MAGIC However, Scala is going to return an object of type `Any`. In Java, this would be an object of type `Object`.
# MAGIC
# MAGIC What we need (at least for Scala), especially if the data type matters in cases of performing mathematical operations on the value, we need to call one of the other methods:
# MAGIC * `getAs[T](i):T`
# MAGIC * `getDate(i):Date`
# MAGIC * `getString(i):String`
# MAGIC * `getInt(i):Int`
# MAGIC * `getLong(i):Long`
# MAGIC
# MAGIC We can now put it all together to get the number of requests for the most requested project:

# COMMAND ----------

article = firstRow['article']
total = firstRow['requests']

print("Most Requested Article: \"{0}\" with {1:,} requests".format( article, total ))

# COMMAND ----------

# This doesn't work
# article = firstRow.get(0)
# total = firstRow.getInt(1)

# print("Most Requested Article: \"{0}\" with {1:,} requests".format( article, total ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) collect()
# MAGIC
# MAGIC If you look at the API docs, `collect(..)` is described like this:
# MAGIC > Returns an array that contains all of Rows in this Dataset.
# MAGIC
# MAGIC `collect()` returns a collection of the specific type backing each record of the `DataFrame`.
# MAGIC * In the case of Python, this is always the `Row` object.
# MAGIC * In the case of Scala, this is also a `Row` object.
# MAGIC * If the `DataFrame` was converted to a `Dataset` the backing object would be the user-specified object.
# MAGIC
# MAGIC Building on our last example, let's take the top 10 records and print them out.

# COMMAND ----------

rows = (articlesDF
  .limit(10)           # We only want the first 10 records.
  .collect()           # The action returning all records in the DataFrame
)

# rows is an Array. Now in the driver, 
# we can just loop over the array and print 'em out.

listItems = ""
for row in rows:
  project = row['article']
  total = row['requests']
  listItems += "    <li><b>{}</b> {:0,d} requests</li>\n".format(project, total)
  
html = """
<body>
  <h1>Top 10 Articles</h1>
  <ol>
    %s
  </ol>
</body>
""" % (listItems.strip())

print(html)

# UNCOMMENT FOR A PRETTIER PRESENTATION
# displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) take(n)
# MAGIC
# MAGIC If you look at the API docs, `take(n)` is described like this:
# MAGIC > Returns the first n rows in the Dataset.
# MAGIC
# MAGIC `take(n)` returns a collection of the first N records of the specific type backing each record of the `DataFrame`.
# MAGIC * In the case of Python, this is always the `Row` object.
# MAGIC * In the case of Scala, this is also a `Row` object.
# MAGIC * If the `DataFrame` was converted to a `Dataset` the backing object would be the user-specified object.
# MAGIC
# MAGIC In short, it's the same basic function as `collect()` except you specify as the first parameter the number of records to return.

# COMMAND ----------

rows = articlesDF.take(10)

# rows is an Array. Now in the driver, 
# we can just loop over the array and print 'em out.

listItems = ""
for row in rows:
  project = row['article']
  total = row['requests']
  listItems += "    <li><b>{}</b> {:0,d} requests</li>\n".format(project, total)
  
html = """
<body>
  <h1>Top 10 Articles</h1>
  <ol>
    %s
  </ol>
</body>
""" % (listItems.strip())

print(html)

# UNCOMMENT FOR A PRETTIER PRESENTATION
# displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DataFrame vs Dataset
# MAGIC
# MAGIC We've been alluding to `Datasets` off and on. 
# MAGIC
# MAGIC The following example demonstrates how to convert a `DataFrame` to a `Dataset`.
# MAGIC
# MAGIC And when compared to the previous example, helps to illustrate the difference/relationship between the two.
# MAGIC
# MAGIC ** *Note:* ** *As a reminder, `Datasets` are a Java and Scala concept and brings to those languages the type safety that *<br/>
# MAGIC *is lost with `DataFrame`, or rather, `Dataset[Row]`. Python and R have no such concept because they are loosely typed.*

# COMMAND ----------

# MAGIC %md
# MAGIC Before we demonstrate this, let's review all our transformations:

# COMMAND ----------

# MAGIC %scala
# MAGIC // val (source, sasEntity, sasToken) = getAzureDataSource()
# MAGIC // spark.conf.set(sasEntity, sasToken)
# MAGIC
# MAGIC val source = "/mnt/training"
# MAGIC val parquetFile = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val articlesDF = spark                          // Our SparkSession & Entry Point
# MAGIC   .read                                         // Our DataFrameReader
# MAGIC   .parquet(parquetFile)                         // Creates a DataFrame from a parquet file
# MAGIC   .filter( $"project" === "en")                 // Include only the "en" project
# MAGIC   .filter($"article" =!= "Main_Page")           // Exclude the Wikipedia Main Page
# MAGIC   .filter($"article" =!= "-")                   // Exclude some "weird" article
# MAGIC   .filter( ! $"article".startsWith("Special:")) // Exclude all the "special" articles
# MAGIC   .drop("bytes_served")                         // We just don't need this column
# MAGIC   .orderBy( $"requests".desc )                  // Sort by requests descending

# COMMAND ----------

# MAGIC %md
# MAGIC Notice above that `articlesDF` is a `Dataset` of type `Row`.
# MAGIC
# MAGIC Next, create the case class `WikiReq`. 
# MAGIC
# MAGIC A little later we can convert this `DataFrame` to a `Dataset` of type `WikiReq`:

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // the name and data type of the case class must match the schema they will be converted from.
# MAGIC case class WikiReq (project:String, article:String, requests:Int)
# MAGIC
# MAGIC articlesDF.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC Instead of the `Row` object, we can now back each record with our new `WikiReq` class.
# MAGIC
# MAGIC And we can see the conversion from `DataFrames` to `Datasets` here:

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val articlesDS = articlesDF.as[WikiReq]

# COMMAND ----------

# MAGIC %md
# MAGIC Make note of the data type: **org.apache.spark.sql.Dataset[WikiReq]**
# MAGIC
# MAGIC Compare that to a `DataFrame`: **org.apache.spark.sql.Dataset[Row]**
# MAGIC
# MAGIC Now when we ask for the first 10, we won't get an array of `Row` objects but instead an array of `WikiReq` objects:

# COMMAND ----------

# MAGIC %scala
# MAGIC val wikiReqs = articlesDS.take(10)
# MAGIC
# MAGIC // wikiReqs is an Array of WikiReqs. Now in the driver, 
# MAGIC // we can just loop over the array and print 'em out.
# MAGIC
# MAGIC var listItems = ""
# MAGIC for (wikiReq <- wikiReqs) {
# MAGIC   // Notice how we don't relaly need temp variables?
# MAGIC   // Or more specifically, we don't need to cast.
# MAGIC   listItems += "    <li><b>%s</b> %,d requests</li>%n".format(wikiReq.article, wikiReq.requests)
# MAGIC }
# MAGIC
# MAGIC var html = s"""
# MAGIC <body>
# MAGIC   <h1>Top 10 Articles</h1>
# MAGIC   <ol>
# MAGIC     ${listItems.trim()}
# MAGIC   </ol>
# MAGIC </body>
# MAGIC """
# MAGIC
# MAGIC println(html)
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %scala
# MAGIC // UNCOMMENT FOR A PRETTIER PRESENTATION
# MAGIC displayHTML(html)

# Databricks notebook source
# MAGIC %md
# MAGIC # This Notebook performs copy activity and copy PSV file to SQL DB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Python:

# COMMAND ----------

storage_account_name = "mystorageadb"
container_name = "adf-data"
storage_account_access_key = "P3TaylkiquB7sW2AvmMOh0fZhBKQBdTK1wCNOul28ulSLjI5Nx+Q4V74LIpsDgdTlwiopoODthebTQBYiGcCuA=="

file_location = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net/CustomerSource.csv"
file_type = "csv"
print(file_location)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# Here we define the schema using a DDL-formatted string (the SQL Data Definition Language).
psv_schema = "CustomerID integer, Title string, FirstName string, MiddleName string, LastName string, Suffix string, CompanyName string, SalesPerson string, EmailAddress string, Phone string"

"""
from pyspark.sql.types import *
psv_schema = StructType([
  StructField("CustomerID", IntegerType(), True),
  StructField("Title", StringType(), True),
  StructField("FirstName", StringType(), True),
  StructField("MiddleName", StringType(), True),
  StructField("LastName", StringType(), True),
  StructField("Suffix", StringType(), True),
  StructField("CompanyName", StringType(), True),
  StructField("SalesPerson", StringType(), True),
  StructField("EmailAddress", StringType(), True),
  StructField("Phone", StringType(), True)
])
"""
print(psv_schema)

# COMMAND ----------

# df = spark.read.format(file_type).option("header", "false").option("sep", "|").schema(psv_schema).load(file_location)

pydf = (spark.read
      .option("header", "false")
      .option("sep", "|")
      .schema(psv_schema)
      .csv(file_location)
     )
display(pydf)

# COMMAND ----------

jdbcUsername = "Vishal"
jdbcPassword = "V@Pass123"

# COMMAND ----------

# Create JDBC URL
jdbcHostname = "sqlserver202124.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "adfsql20210624"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

connectionProperties = {
 "user" : jdbcUsername,
 "password" : jdbcPassword }

print(connectionProperties)
print(jdbcUrl)

# COMMAND ----------

# Transforming the data
Transformedmydf = pydf
# transformedmydf = pydf.withColumnRenamed("CustomerID", "CustomerUniqueID")
# display(transformedmydf)

# COMMAND ----------

# Write data to Azure SQL DB
Transformedmydf.write.jdbc(url=jdbcUrl, table= "CustomerSource", mode ="overwrite", properties = connectionProperties)

# COMMAND ----------

# Read data from Azure SQL DB
azsqldbtable = spark.read.jdbc(url=jdbcUrl, table= "CustomerSource", properties = connectionProperties)
display(azsqldbtable.select("SalesPerson").groupBy("SalesPerson").count())


# COMMAND ----------

dbutils.notebook.exit("Executed using Python")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Scala:

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.mystorageadb.blob.core.windows.net",
# MAGIC   "P3TaylkiquB7sW2AvmMOh0fZhBKQBdTK1wCNOul28ulSLjI5Nx+Q4V74LIpsDgdTlwiopoODthebTQBYiGcCuA==")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val psv_schema = StructType(List(
# MAGIC   StructField("CustomerID", StringType),
# MAGIC   StructField("Title", StringType),
# MAGIC   StructField("FirstName", StringType),
# MAGIC   StructField("MiddleName", StringType),
# MAGIC   StructField("LastName", StringType),
# MAGIC   StructField("Suffix", StringType),
# MAGIC   StructField("CompanyName", StringType),
# MAGIC   StructField("SalesPerson", StringType),
# MAGIC   StructField("EmailAddress", StringType),
# MAGIC   StructField("Phone", StringType)
# MAGIC   ))
# MAGIC 
# MAGIC val scaladf = spark.read.format("csv")
# MAGIC     .option("header","false")
# MAGIC     .option("sep", "|")
# MAGIC     .schema(psv_schema)
# MAGIC     .load("wasbs://adf-data@mystorageadb.blob.core.windows.net/CustomerSource.csv")
# MAGIC display(scaladf)

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcUsername = "Vishal"
# MAGIC val jdbcPassword = "V@Pass123"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Check that the JDBC driver is available
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %scala
# MAGIC //Create the JDBC URL
# MAGIC val jdbcHostname = "sqlserver202124.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "adfsql20210624"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC 
# MAGIC //check the connectivity to the SQL Server Database
# MAGIC //val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC //connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC //Transforming the data
# MAGIC val Transformedmydf = scaladf
# MAGIC // val transformedmydf = scaladf.withColumnRenamed("CustomerID", "CustomerUniqueID")
# MAGIC // display(transformedmydf)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Write data to Azure SQL DB
# MAGIC Transformedmydf.write.mode("overwrite").jdbc(jdbcUrl, "CustomerSource", connectionProperties)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read data from Azure SQL DB
# MAGIC val azsqldbtable = spark.read.jdbc(jdbcUrl, "CustomerSource", connectionProperties)
# MAGIC display(azsqldbtable.select("SalesPerson").groupBy("SalesPerson").count())

# COMMAND ----------

dbutils.notebook.exit("Executed using Scala")

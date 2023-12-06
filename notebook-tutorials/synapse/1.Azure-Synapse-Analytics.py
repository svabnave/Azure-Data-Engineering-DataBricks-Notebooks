# Databricks notebook source
# MAGIC %md
# MAGIC # Reading and Writing to Synapse
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the connection architecture of Synapse and Spark
# MAGIC * Configure a connection between Databricks and Synapse
# MAGIC * Read data from Synapse
# MAGIC * Write data to Synapse
# MAGIC 
# MAGIC ### Azure Synapse
# MAGIC - leverages massively parallel processing (MPP) to quickly run complex queries across petabytes of data
# MAGIC - PolyBase T-SQL queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synapse Connector
# MAGIC - uses Azure Blob Storage as intermediary
# MAGIC - uses PolyBase in Synapse
# MAGIC - enables MPP reads and writes to Synapse from Azure Databricks
# MAGIC 
# MAGIC Note: The Synapse connector is more suited to ETL than to interactive queries. For interactive and ad-hoc queries, data should be extracted into a Databricks Delta table.
# MAGIC 
# MAGIC ```
# MAGIC                            ┌─────────┐
# MAGIC       ┌───────────────────>│ STORAGE │<──────────────────┐
# MAGIC       │ Storage acc key /  │ ACCOUNT │ Storage acc key / │
# MAGIC       │ Managed Service ID └─────────┘ OAuth 2.0         │
# MAGIC       │                         │                        │
# MAGIC       │                         │ Storage acc key /      │
# MAGIC       │                         │ OAuth 2.0              │
# MAGIC       v                         v                 ┌──────v────┐
# MAGIC ┌──────────┐              ┌──────────┐            │┌──────────┴┐
# MAGIC │ Synapse  │              │  Spark   │            ││ Spark     │
# MAGIC │ Analytics│<────────────>│  Driver  │<───────────>| Executors │
# MAGIC └──────────┘  JDBC with   └──────────┘ Configured  └───────────┘
# MAGIC               username &               in Spark
# MAGIC               password
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL DW Connection
# MAGIC 
# MAGIC Three connections are made to exchange queries and data between Databricks and Synapse
# MAGIC 1. **Spark driver to Synapse**
# MAGIC    - the Spark driver connects to Synapse via JDBC using a username and password
# MAGIC 2. **Spark driver and executors to Azure Blob Storage**
# MAGIC    - the Azure Blob Storage container acts as an intermediary to store bulk data when reading from or writing to Synapse
# MAGIC    - Spark connects to the Blob Storage container using the Azure Blob Storage connector bundled in Databricks Runtime
# MAGIC    - the URI scheme for specifying this connection must be wasbs
# MAGIC    - the credential used for setting up this connection must be a storage account access key
# MAGIC    - the account access key is set in the session configuration associated with the notebook that runs the command
# MAGIC    - this configuration does not affect other notebooks attached to the same cluster. `spark` is the SparkSession object provided in the notebook
# MAGIC 3. **Synapse to Azure Blob Storage**
# MAGIC    - Synapse also connects to the Blob Storage container during loading and unloading of temporary data
# MAGIC    - set `forwardSparkAzureStorageCredentials` to true
# MAGIC    - the forwarded storage access key is represented by a temporary database scoped credential in the Synapse instance
# MAGIC    - Synapse connector creates a database scoped credential before asking Synapse to load or unload data
# MAGIC    - then it deletes the database scoped credential once the loading or unloading operation is done.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enter Variables from Cloud Setup
# MAGIC 
# MAGIC Before starting this lesson, you were guided through configuring Azure Synapse and deploying a Storage Account and blob container.
# MAGIC 
# MAGIC In the cell below, enter the **Storage Account Name**, the **Container Name**, and the **Access Key** for the blob container you created.
# MAGIC 
# MAGIC Also enter the JDBC connection string for your Azure Synapse instance. Make sure you substitute in your password as indicated within the generated string.

# COMMAND ----------

storageAccount = "adlsynapsebricks"
containerName = "synapsefs"
accessKey = "IsaX3WxvWbN4TaRf6LOPcIwDZGDjsGi+Uft6fuEvGaEJBFPiu4KI/KJ0Mv5gKPiLPFpa17DmXC8I1mfus0x8MQ=="
jdbcURI = "jdbc:sqlserver://mysynapsefordatabricks.sql.azuresynapse.net:1433;database=SQLPool1;user=Vishal@mysynapsefordatabricks;password=V@Pass123;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

spark.conf.set(f"fs.azure.account.key.{storageAccount}.blob.core.windows.net", accessKey)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from the Customer Table
# MAGIC 
# MAGIC Next, use the Synapse Connector to read data from the Customer Table.
# MAGIC 
# MAGIC Use the read to define a tempory table that can be queried.
# MAGIC 
# MAGIC Note:
# MAGIC 
# MAGIC - the connector uses a caching directory on the Azure Blob Container.
# MAGIC - `forwardSparkAzureStorageCredentials` is set to `true` so that the Synapse instance can access the blob for its MPP read via Polybase

# COMMAND ----------

cacheDir = f"wasbs://{containerName}@{storageAccount}.blob.core.windows.net/cacheDir"

tableName = "dbo.DimCustomer5"

customerDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName)
  .load())

customerDF.createOrReplaceTempView("customer_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Use SQL queries to count the number of rows in the Customer table and to display table metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC describe customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC Note that `CurrencyKey` and `CurrencyAlternateKey` use a very similar naming convention.

# COMMAND ----------

# MAGIC %sql
# MAGIC select CurrencyKey, CurrencyAlternateKey from customer_data limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC In a situation in which we may be merging many new customers into this table, we can imagine that we may have issues with uniqueness with regard to the `CurrencyKey`. Let us redefine `CurrencyAlternateKey` for stronger uniqueness using a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).
# MAGIC 
# MAGIC To do this we will define a UDF and use it to transform the `CurrencyAlternateKey` column. Once this is done, we will write the updated Customer Table to a Staging table.
# MAGIC 
# MAGIC **Note:** It is a best practice to update the Synapse instance via a staging table.

# COMMAND ----------

import uuid

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CurrencyAlternateKey", uuidUdf())
display(customerUpdatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the Polybase Connector to Write to the Staging Table

# COMMAND ----------

(customerUpdatedDF.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url", jdbcURI)
  .option("forward_spark_azure_storage_credentials", "true")
  .option("dbtable", tableName + "Staging")
  .option("tempdir", cacheDir)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Display Changes from Staging Table

# COMMAND ----------

customerTempDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName + "Staging")
  .load())

customerTempDF.createOrReplaceTempView("customer_temp_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select CurrencyKey, CurrencyAlternateKey from customer_temp_data limit 10;

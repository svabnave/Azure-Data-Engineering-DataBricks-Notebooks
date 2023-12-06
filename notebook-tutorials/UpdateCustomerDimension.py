# Databricks notebook source
# MAGIC %md
# MAGIC # This Notebook performs Data Flow Transformation:
# MAGIC ### To update Dimensions in CustomerSource SQL Table and write final Transformed data to DimCustomer SQL Table

# COMMAND ----------

# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC 
# MAGIC val userhome = s"dbfs:/user/$username"
# MAGIC 
# MAGIC // Set the user's name and home directory
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
# MAGIC 
# MAGIC print(username, userhome)

# COMMAND ----------

# Transfer variables from Scala to Python
username = spark.conf.get("com.databricks.training.username")
userhome = spark.conf.get("com.databricks.training.userhome")
print(username, userhome)

# COMMAND ----------

# Create JDBC URL
jdbcHostname = "sqlserver202124.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "adfsql20210624"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

connectionProperties = {
 "user" : "Vishal",
 "password" : "V@Pass123" }

print(connectionProperties)
print(jdbcUrl)

# COMMAND ----------

# Read data from Azure SQL DB
sqldf = spark.read.jdbc(url=jdbcUrl, table= "CustomerSource", properties = connectionProperties)
# display(sqldf)

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat_ws,sha2
"""
HashKeydf = (sqldf
             .select(
               'CustomerID','Title','FirstName','MiddleName','LastName','Suffix','CompanyName','SalesPerson','EmailAddress','Phone',
               sha2(concat_ws('',col("Title"),col("FirstName"),col("MiddleName"), col("LastName"), col("Suffix"), col("CompanyName"), col("SalesPerson"), col("EmailAddress"), col("Phone") ), 256).alias("HashKey")              
             )
            )
"""

HashKeydf = sqldf.withColumn('HashKey', sha2(concat_ws('',col("Title"),col("FirstName"),col("MiddleName"), col("LastName"), col("Suffix"), col("CompanyName"), col("SalesPerson"), col("EmailAddress"), col("Phone") ), 256))

display(HashKeydf)
#hashlib.sha256(  +iifNull(Suffix,'') +iifNull(CompanyName,'') +iifNull(SalesPerson,'') +iifNull(EmailAddress,'') +iifNull(Phone,''))).hexdigest()

# COMMAND ----------

dimdf = spark.read.jdbc(url=jdbcUrl, table= "DimCustomer", properties = connectionProperties)
# display(dimdf)

# COMMAND ----------

HashKeydf.createOrReplaceTempView("Hash_Key_Source")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Hash_Key_Source;

# COMMAND ----------

DestinationDeltaPath = userhome + "/delta/dim_customer/"
dbutils.fs.rm(DestinationDeltaPath, True) #deletes Delta table if previously created

(dimdf.write
 .mode("overwrite")
 .format("delta")
 # .option("partitionby","Country")
 # .partitionBy("Country")
 .save(DestinationDeltaPath)
)

spark.sql("""
  DROP TABLE IF EXISTS Dim_Sink
""")

spark.sql("""
  CREATE TABLE Dim_Sink
  USING DELTA
  LOCATION '{}'
""".format(DestinationDeltaPath))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO Dim_Sink T
# MAGIC USING Hash_Key_Source S
# MAGIC ON T.CustomerID = S.CustomerID
# MAGIC WHEN MATCHED  AND T.HashKey != S.HashKey THEN   /* It means there is some change in data. we need to update DIM table in Sink */
# MAGIC UPDATE SET Title = S.Title, FirstName = S.FirstName, MiddleName = S.MiddleName, LastName = S.LastName, Suffix = S.Suffix, CompanyName = S.CompanyName, SalesPerson = S.SalesPerson, EmailAddress = S.EmailAddress, Phone = S.Phone, HashKey = S.HashKey, ModifiedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (CustomerID, Title, FirstName, MiddleName, LastName, Suffix, CompanyName, SalesPerson, EmailAddress, Phone, HashKey, InsertedDate, ModifiedDate) 
# MAGIC VALUES (S.CustomerID, S.Title, S.FirstName, S.MiddleName, S.LastName, S.Suffix, S.CompanyName, S.SalesPerson, S.EmailAddress, S.Phone, S.HashKey, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Dim_Sink

# COMMAND ----------

# Write transformed data to Azure SQL DB
Transformeddf = spark.sql("select * from Dim_Sink")
Transformeddf.write.jdbc(url=jdbcUrl, table= "DimCustomer", mode ="overwrite", properties = connectionProperties)

# COMMAND ----------

# Read data from DimCustomer
azsqldbtabledf = spark.read.jdbc(url=jdbcUrl, table= "DimCustomer", properties = connectionProperties)
display(azsqldbtabledf)


# COMMAND ----------

dbutils.notebook.exit("Successfully wrote DimCustomer table")

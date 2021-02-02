// Databricks notebook source
// Imports
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// COMMAND ----------

// Create parameters
dbutils.widgets.text("pPartition","2021/02/01/")

// Read parameters
val pPartition = dbutils.widgets.get("pPartition").toString

// Variables
val storage = "" // enter the name of your storage account here
val container = "staging"
val partition = s"posts/$pPartition"

// Storage account key
val key = "" // enter the key to access your storage account here

// Set configuration
spark.conf.set(
    s"fs.azure.account.key.$storage.blob.core.windows.net"
    ,key)

// COMMAND ----------

// Load dataframe with data from all files
val df = spark
  .read.parquet(s"wasbs://$container@$storage.blob.core.windows.net/$partition")                  


// COMMAND ----------

// Transform data before saving to staging container
val dfByTitle = df
  .select(
    $"title"
  )
  .groupBy(
    $"title"
  )
  .agg(count(lit(1)).alias("count"))


// COMMAND ----------

// Save transformed data as parquet in curated
dfByTitle.write.mode(SaveMode.Overwrite).parquet(s"wasbs://curated@$storage.blob.core.windows.net/$partition/users-by-title")

dbutils.notebook.exit("success")

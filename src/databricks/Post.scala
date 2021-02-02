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
val dfLikesByUser = df
  .select(
    $"firstName",
    $"lastName",
    $"likes"
  )
  .groupBy(
    $"firstName",
    $"lastName"
  )
  .agg(
      avg("likes").alias("averageLikes"),
      count(lit(1)).alias("count")
  )


// COMMAND ----------

// Save transformed data as parquet in curated
dfLikesByUser.write.mode(SaveMode.Overwrite).parquet(s"wasbs://curated@$storage.blob.core.windows.net/$partition/posts-likes-by-user")

dbutils.notebook.exit("success")

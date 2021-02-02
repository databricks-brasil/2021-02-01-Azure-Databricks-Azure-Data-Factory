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
val partition = s"$pPartition"


// COMMAND ----------


val statusUser = dbutils.notebook.run("User", timeoutSeconds = 300, arguments = Map("pPartition" -> partition))
println(statusUser)

val statusPost = dbutils.notebook.run("Post", timeoutSeconds = 300, arguments = Map("pPartition" -> partition))
println(statusPost)


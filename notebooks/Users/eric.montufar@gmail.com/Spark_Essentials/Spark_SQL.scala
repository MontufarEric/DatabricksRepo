// Databricks notebook source
// MAGIC %md
// MAGIC ## Spark SQL
// MAGIC 
// MAGIC SQL is supported programatically in expressions (selectExpr)
// MAGIC and Spark shell. 
// MAGIC 
// MAGIC Allows us to access database tables and dataframes. 
// MAGIC 
// MAGIC Tables can be External and Managed: 
// MAGIC 
// MAGIC - External: Spark only manges the metadata and points to a file somewhere else. Dropping an external table only deletes the metadata. 
// MAGIC 
// MAGIC - Managed: Spark is in charge of data and metadata. Dropping it will delete everything. 

// COMMAND ----------

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

// COMMAND ----------

val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

// COMMAND ----------

// regular DF API
carsDF.select(col("Name")).where(col("Origin") === "USA")

// COMMAND ----------

// Creates a table from a df to use as a SQL table
carsDF.createOrReplaceTempView("cars")

// creating a DF from a SQL query usign the view we just created 
val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin)

// COMMAND ----------

// we can run ANY SQL statement
// when creating a table or db Spark stores it into Spark Warehouse. We can mdify that in the config to save it anywhere

spark.sql("create database rtjvm")
spark.sql("use rtjvm")
val databasesDF = spark.sql("show databases")

// COMMAND ----------

// transfer tables from a DB to Spark tables
val driver = "org.postgresql.Driver"
val url = "jdbc:postgresql://localhost:5432/rtjvm"
val user = "docker"
val password = "docker"

// COMMAND ----------

def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

// COMMAND ----------

//  Transfering tables from a database to spark sql tables

def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

// COMMAND ----------

transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

// COMMAND ----------

// read DF from loaded Spark tables
val employeesDF2 = spark.read.table("employees")


// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


// Databricks notebook source
// MAGIC %md
// MAGIC ## Spark RDDs
// MAGIC 
// MAGIC Distributed typed collections of JvM objects. Lowest level API in Spark. 
// MAGIC They can be highly optimized. But they require you to know the spark internals very well and everything has to be manually tuned. 
// MAGIC 
// MAGIC - Dataframes are auto-optimized by Spark. 
// MAGIC - RDDs and Datasets can use map, flatmap, reduce, take, ...
// MAGIC - aggregations, groupby, sortby
// MAGIC 
// MAGIC RDDs  only --> 
// MAGIC                -  partition control (repartition, coalesce, zipPartitions, mapPartitions)
// MAGIC                -  operation control (checkpointing, isCheckpointed, localCheckpoint, chache)
// MAGIC                -  storage control (cache, getStorageLevel, persist)
// MAGIC                
// MAGIC Datasets only -->
// MAGIC                   - Select and join
// MAGIC                   - Already optimized (planning)
// MAGIC                                     

// COMMAND ----------

// the SparkContext is the entry point for low-level APIs, including RDDs
val sc = spark.sparkContext

// 1 - parallelize an existing collection
val numbers = 1 to 1000000
val numbersRDD = sc.parallelize(numbers)

// COMMAND ----------

// 2 - reading from files
case class StockValue(symbol: String, date: String, price: Double)
def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

// COMMAND ----------

 val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

// COMMAND ----------

// 2b - reading from files
val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

// COMMAND ----------

// 3 - read from a DF
val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

// turn it into an Dataset and then to an RDD of StockValue
// It is also possible to do it from DF to RDD directly but it will be an RDD[Row]

import spark.implicits._
val stocksDS = stocksDF.as[StockValue]
val stocksRDD3 = stocksDS.rdd

// COMMAND ----------

// RDD -> DF
val numbersDF = numbersRDD.toDF("numbers") // you lose the type info

// COMMAND ----------

// RDD -> DS
val numbersDS = spark.createDataset(numbersRDD) // you get to keep type info

// COMMAND ----------

// Transformations

// distinct
val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
val msCount = msftRDD.count() // eager ACTION

// COMMAND ----------

// counting
val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

// COMMAND ----------

// min and max
implicit val stockOrdering: Ordering[StockValue] =
Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
val minMsft = msftRDD.min() // action

// COMMAND ----------

// reduce
numbersRDD.reduce(_ + _)

// COMMAND ----------

// grouping
val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
// ^^ very expensive

// COMMAND ----------

// Partitioning
val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /*
    Repartitioning is EXPENSIVE. Involves Shuffling.
    Best practice: partition EARLY, then process that.
    Size of a partition 10-100MB.
   */

// COMMAND ----------

// coalesce
val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

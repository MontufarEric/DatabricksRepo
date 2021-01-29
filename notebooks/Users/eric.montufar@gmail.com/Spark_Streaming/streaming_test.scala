// Databricks notebook source
val ssc = new StreamingContext(conf, Seconds(1))

// COMMAND ----------

val lines = ssc.socketTextStream("localhost", 9999)

// COMMAND ----------

val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)

// COMMAND ----------

wordCounts.print()

// COMMAND ----------


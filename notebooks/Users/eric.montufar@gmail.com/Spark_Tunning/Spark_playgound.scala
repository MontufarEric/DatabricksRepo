// Databricks notebook source
val numbers = spark.range(1000000)

// COMMAND ----------

val times5 = numbers.selectExpr("id * as id")

// COMMAND ----------

times5.explain()

// COMMAND ----------

val numbers7 = numbers.repartition(7).excplain()

// COMMAND ----------

ds1 = val ds1 = spark.range(1, 10000000)
val ds2 = spark.range(1, 20000000, 2)
val ds3 = ds1.repartition(7)
val ds4 = ds2.repartition(9)
val ds5 = ds3.selectExpr("id * 3 as id")
val joined = ds5.join(ds4, "id")
val sum = joined.selectExpr("sum(id)")


// COMMAND ----------

sum.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ### pivoting a table 

// COMMAND ----------

val flights = sqlContext
  .read
  .format("csv")
  .options(Map("inferSchema" -> "true", "header" -> "true"))
  .load("flights.csv")

flights
  .groupBy($"origin", $"dest", $"carrier")
  .pivot("hour")
  .agg(avg($"arr_delay"))

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMPORARY VIEW flights 
// MAGIC USING csv 
// MAGIC OPTIONS (header 'true', path 'flights.csv', inferSchema 'true') ;
// MAGIC 
// MAGIC  SELECT * FROM (
// MAGIC    SELECT origin, dest, carrier, arr_delay, hour FROM flights
// MAGIC  ) PIVOT (
// MAGIC    avg(arr_delay)
// MAGIC    FOR hour IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
// MAGIC                 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)
// MAGIC  );

// COMMAND ----------


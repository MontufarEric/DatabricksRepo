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
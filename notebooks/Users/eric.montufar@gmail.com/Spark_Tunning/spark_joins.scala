// Databricks notebook source
// MAGIC %md 
// MAGIC ## Broadcast Join

// COMMAND ----------

val peopleDF = Seq(
  ("andrea", "medellin"),
  ("rodolfo", "medellin"),
  ("abdul", "bangalore")
).toDF("first_name", "city")

peopleDF.show()

// COMMAND ----------

val citiesDF = Seq(
  ("medellin", "colombia", 2.5),
  ("bangalore", "india", 12.3)
).toDF("city", "country", "population")

citiesDF.show()

// COMMAND ----------

peopleDF.join(
  broadcast(citiesDF),
  peopleDF("city") <=> citiesDF("city")
).show()

// COMMAND ----------



// COMMAND ----------


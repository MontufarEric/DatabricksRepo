// Databricks notebook source
import spark.implicits._

// COMMAND ----------

val df1 = sc.parallelize(
  Seq.fill(1000000){("Seinfeld, Jerry", "New York")}
).toDF("name", "state")

val df2 = sc.parallelize(
  Seq.fill(1000000){("David, Larry", "NY")}
).toDF("name", "state")

val someDF = df1.union(df2)

// COMMAND ----------

someDF.write.format("delta").mode("overwrite").save("s3a://filestoragedatabricks/seinfield_test_cdc_delta/bronze")

// COMMAND ----------

import org.apache.spark.sql.functions.{col,split}

//Read the table as a stream
val bronzeData = spark.readStream.format("delta").load("s3a://filestoragedatabricks/seinfield_test_cdc_delta/bronze")

//Perform name parsing
val bronzeDataCleaned = bronzeData.withColumn("_tmp", split(col("name"), ",")).select(
  col("_tmp").getItem(0).as("last"),
  col("_tmp").getItem(1).as("first"),
  col("state")).drop(col("_tmp"))

//Write into Silver table
bronzeDataCleaned.writeStream.format("delta")
            .option("checkpointLocation","s3a://filestoragedatabricks/check_point")
            .start("s3a://filestoragedatabricks/silver")

// COMMAND ----------

val silverData = spark.read.format("delta").load("s3a://filestoragedatabricks/silver")
display(silverData)

// COMMAND ----------

display(silverData.groupBy("last").count())

// COMMAND ----------

val moreDF = sc.parallelize(
  Seq.fill(1000000){("cosmo, kramer", "New York")}
).toDF("name", "state")


moreDF.write.format("delta").mode("append").save("s3a://filestoragedatabricks/seinfield_test_cdc_delta/bronze")

// COMMAND ----------


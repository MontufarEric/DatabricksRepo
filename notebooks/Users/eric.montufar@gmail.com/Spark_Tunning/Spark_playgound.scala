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

// python
from pyspark.sql.functions import avg

flights = (sqlContext
    .read
    .format("csv")
    .options(inferSchema="true", header="true")
    .load("flights.csv")
    .na.drop())

flights.registerTempTable("flights")
sqlContext.cacheTable("flights")

gexprs = ("origin", "dest", "carrier")
aggexpr = avg("arr_delay")

flights.count()
## 336776

%timeit -n10 flights.groupBy(*gexprs ).pivot("hour").agg(aggexpr).count()
## 10 loops, best of 3: 1.03 s per loop

// COMMAND ----------

// sample data 
"year","month","day","dep_time","sched_dep_time","dep_delay","arr_time","sched_arr_time","arr_delay","carrier","flight","tailnum","origin","dest","air_time","distance","hour","minute","time_hour"
2013,1,1,517,515,2,830,819,11,"UA",1545,"N14228","EWR","IAH",227,1400,5,15,2013-01-01 05:00:00
2013,1,1,533,529,4,850,830,20,"UA",1714,"N24211","LGA","IAH",227,1416,5,29,2013-01-01 05:00:00
2013,1,1,542,540,2,923,850,33,"AA",1141,"N619AA","JFK","MIA",160,1089,5,40,2013-01-01 05:00:00
2013,1,1,544,545,-1,1004,1022,-18,"B6",725,"N804JB","JFK","BQN",183,1576,5,45,2013-01-01 05:00:00
2013,1,1,554,600,-6,812,837,-25,"DL",461,"N668DN","LGA","ATL",116,762,6,0,2013-01-01 06:00:00
2013,1,1,554,558,-4,740,728,12,"UA",1696,"N39463","EWR","ORD",150,719,5,58,2013-01-01 05:00:00
2013,1,1,555,600,-5,913,854,19,"B6",507,"N516JB","EWR","FLL",158,1065,6,0,2013-01-01 06:00:00
2013,1,1,557,600,-3,709,723,-14,"EV",5708,"N829AS","LGA","IAD",53,229,6,0,2013-01-01 06:00:00
2013,1,1,557,600,-3,838,846,-8,"B6",79,"N593JB","JFK","MCO",140,944,6,0,2013-01-01 06:00:00
2013,1,1,558,600,-2,753,745,8,"AA",301,"N3ALAA","LGA","ORD",138,733,6,0,2013-01-01 06:00:00


// COMMAND ----------

class simpleCSVHeader(header: Array[String] extends Serializable) {
  val index = header.zipWithIndex.toMap
  def apply(array: Array[String],key: String) : String = array(index(key))
}

// COMMAND ----------

val csv = sc.textFile(""s3a://filestoragedatabricks/Iris.csv"")
val data = csv.map(line => line.split(",").map(elem => elem.trim))

// COMMAND ----------

val header = 

// COMMAND ----------



// COMMAND ----------


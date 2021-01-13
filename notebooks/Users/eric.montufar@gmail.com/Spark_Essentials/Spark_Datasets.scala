// Databricks notebook source
// MAGIC %md
// MAGIC ## Datasets
// MAGIC 
// MAGIC In Dataframes we used a collections of rows to represent data. Here we use a collection of JVM objects. 
// MAGIC 
// MAGIC - This enforces object type
// MAGIC - Increases code safety and consitency
// MAGIC - Allows us to use filter and transformations hard to express in DF/SQL
// MAGIC 
// MAGIC BUT:
// MAGIC 
// MAGIC - Trasformations are not optimized 
// MAGIC - DFs are faster

// COMMAND ----------

val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

// COMMAND ----------

numbersDF.printSchema()

// COMMAND ----------

// convert a DF to a Dataset
// encodes a DF row into an int 

implicit val intEncoder = Encoders.scalaInt
val numbersDS: Dataset[Int] = numbersDF.as[Int]

// COMMAND ----------

// dataset of a complex type
// 1 - define your case class
case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

// COMMAND ----------

// 2 - read the DF from the file
def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

// COMMAND ----------

val carsDF = readDF("cars.json")

// COMMAND ----------

// 3 - define an encoder (importing the implicits)
import spark.implicits._
// 4 - convert the DF to DS identifying each row as a Car object thanks to implicits
val carsDS = carsDF.as[Car]

// COMMAND ----------

// DS collection functions
numbersDS.filter(_ < 100)

// COMMAND ----------

// map, flatMap, fold, reduce, for comprehensions ...
val carNamesDS = carsDS.map(car => car.Name.toUpperCase())


// COMMAND ----------

// MAGIC %md
// MAGIC ### Dataset joins

// COMMAND ----------

// Joins
case class Guitar(id: Long, make: String, model: String, guitarType: String)
case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
case class Band(id: Long, name: String, hometown: String, year: Long)

// COMMAND ----------

val guitarsDS = readDF("guitars.json").as[Guitar]
val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
val bandsDS = readDF("bands.json").as[Band]

// COMMAND ----------

val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

package part3types_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.language.postfixOps

object ComplexTypes extends App {

  val spark = SparkSession.builder().appName("Complex Data Types").config("spark.master", "local").getOrCreate()
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // allow to parse date correctly
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  // Dates manipulations
  moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("release"))
    .withColumn("current_date", current_date())
    .withColumn("current_timestamp", current_timestamp())
    .withColumn("datediff", datediff(col("current_date"), col("release")) / 365)
    .withColumn("+ 10 days", date_add(current_date, 10))
    .withColumn("- 10 days", date_sub(current_date, 10))

  // Structs
  val dfWithStructs = moviesDF.select(col("Title"), struct(col("US_Gross"),
    col("Worldwide_Gross"), col("US_DVD_Sales")).as("profit"))
  dfWithStructs.select(col("profit").getField("US_DVD_Sales"))

  // Arrays
  val dfWithArrays = moviesDF.select(col("Title"), split(col("Title"), " ").as("array"))

  // arrays manipulation examples
  val definedArray = col("array")

  dfWithArrays.select(
    expr("array[0]"),
    size(definedArray),
    array_contains(definedArray,"Tom")
  ).show()



}

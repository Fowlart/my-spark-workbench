package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object DataFramesBasics extends App {

  // Spark session creating
  val spark = SparkSession.builder()
    .appName("Data-Frames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val firstDataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")


  firstDataFrame.show(5)
  firstDataFrame.printSchema(5)
  firstDataFrame.take(5).foreach(row => println(s"==> ${row}"))

  // spark types
  val variable = LongType


  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val usedDataFrameSchema = firstDataFrame.schema
  println(s"[used schema] $usedDataFrameSchema")

  val carsDfWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  carsDfWithSchema.show(10)

  // row creation
  val row = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  // manual dataframe
  val myCars = Seq(("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"))
  val manualDF = spark.createDataFrame(myCars)
  manualDF.show()

  import spark.implicits._
  // with this imports sequences has toDF() method
  myCars.toDF("one", "two", "tree", "four", "five", "six", "seven", "eight", "nine").show()

  //** Ex 1: dataframes to describe something */
  val myPeople = Seq(("Olenka", "1200", "House-keeper", "cooking"), ("Artur", "1800", "Java developer", "running"), ("Zenoviy", "4000", "Senior-House-keeper", "Cars"))

  val smartphonesDf = myPeople.toDF("Name", "Income", "Occupation", "Hobie")
  smartphonesDf.show()

  //** Ex 2: load the movie json */
  val moviesDataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDataFrame.show()
  println("==[SCHEMA]==")
  moviesDataFrame.schema.foreach(el => println(el))
  println(s"count of rows: ${moviesDataFrame.count()}")


}

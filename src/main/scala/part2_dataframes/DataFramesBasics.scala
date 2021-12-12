package part2_dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object DataFramesBasics extends App {

  val sparkSession = SparkSession.builder()
    .appName("Data-Frames basics 2") // find app in the UI
    .config("spark.master", "local") // ability to configure remote spark
    .getOrCreate()

  /** Dataframe is a schema + distributed
    * collection of rows that conforms to the data */

  // defining limited schema will lead for truncating data
  // full constructor
  val carsSchema = StructType(Array(StructField("Acceleration", DoubleType, nullable = true, Metadata.empty)))

  val cars = sparkSession
    .read
    // .option("inferSchema", "true") // do not plays role ?
    .schema(carsSchema)
    .json("src/main/resources/data/cars.json") //

  /** General
    * operations */

  cars.show(5)
  cars.printSchema()
  cars.tail(5).foreach(row => println(row))
  // retrieving schema
  val retrievedCarsSchema = cars.schema
  println(retrievedCarsSchema)
  // creating an row
  val myNewRow = Row(111.11d)

  // creating df from tuples
  // use Seq in order to call implicit toDF() method
  val inputData = Seq(("Artur", 2.8d), ("Olena", 0.65d), ("Serhiy", 4.5d))
  // no way to set schema
  val inputSchema = StructType(Array(StructField("name", StringType, nullable = true, Metadata.empty), StructField("salary", DoubleType, nullable = true, Metadata.empty)))
  sparkSession.createDataFrame(inputData)

  // create DF with implicits, specifying column names

  import sparkSession.implicits._

  inputData.toDF("Name", "Salary").show()
}

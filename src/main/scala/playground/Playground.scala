package playground

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import rdd.Rdds.spark

/**
  * This is a small application that loads some manually inserted rows into a Spark DataFrame.
  * Feel free to modify this code as you see fit, fiddle with the code and play with your own exercises, ideas and datasets.
  *
  * Daniel @ Rock the JVM
  */
object Playground extends App {

  /**
    * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    */
  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs, including access to Resilient Distributed Datasets (RDDs).
    */
  val sc = spark.sparkContext

  /**
    * A Spark schema structure that describes a small cars DataFrame.
    */
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /**
    * A "manual" sequence of rows describing cars, fetched from cars.json in the data folder.
    */
  val cars = Seq(
    Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    Row("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    Row("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    Row("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    Row("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    Row("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    Row("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    Row("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    Row("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    Row("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  /**
    * The two lines below create an RDD of rows (think of an RDD like a parallel collection).
    * Then from the RDD we create a DataFrame, which has a number of useful querying methods.
    */
  val carsRows = sc.parallelize(cars)
  val carsDF = spark.createDataFrame(carsRows, carsSchema)

  /**
    * If the schema and the contents of the DataFrame are printed correctly to the console,
    * this means the libraries work correctly and you can jump into the course!
    */
  carsDF.printSchema()
  carsDF.show()

  /** Playing with Spark shell */

  val firstRdd = sc.parallelize(1 to 1000000000)
  println(firstRdd.getNumPartitions)
  val secondRdd = firstRdd.map(_*2)
  println(secondRdd.count)
  println(s"repartition and count: ${secondRdd.repartition(30).count}")

  import spark.implicits._
  // toDF will not works without implicits
  secondRdd.toDF().show()

  val newRange = spark.range(0,10000,2)
  newRange.show()
  newRange.explain // will print out physical plan of execution

  val newRange2 = spark.range(0,10000,4)
  val repartitionedRange = newRange.repartition(7)
  repartitionedRange.explain() // will print out more complex explain
  val repartitionedRange2 = newRange2.repartition(7)
  repartitionedRange2.explain
  newRange2.selectExpr("id * 5 as id").explain

  newRange2.join(repartitionedRange2,"id").explain
  val complicate  = newRange2.join(repartitionedRange2,"id").selectExpr("sum(id)")
  complicate.explain
}

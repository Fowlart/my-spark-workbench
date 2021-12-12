package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder().appName("Data Sources").config("spark.master", "local").getOrCreate()

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

  val carsDfWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .load("src/main/resources/data/cars.json")

  /** COLUMNS */

  // Select(correct term is `projecting`):
  val nameColumn = carsDfWithSchema.col("Name")
  val carsName = carsDfWithSchema.select(nameColumn)
  carsName.show()

  import spark.implicits._

  // Select many columns in different way
  carsDfWithSchema.select(
    carsDfWithSchema.col("Name"),
    col("Year"),
    column("Year"),
    'Year,
    $"Year",
    expr("Year")).show()

  // simpler
  carsDfWithSchema
    .select("Name", "Year")
    .show(5)

  /** EXPRESSIONS */

  val weightInKgExpr = carsDfWithSchema.col("Weight_in_lbs") / 2.2

  val transformedTable1 = carsDfWithSchema
    .select('Name, weightInKgExpr.as("WEIGHT_IN_KGS"),
      expr("Weight_in_lbs / 2.2").as("WEIGHT IN KGS"))
  transformedTable1.show(5)

  val transformedTable2 = carsDfWithSchema.selectExpr("Name", "Weight_in_lbs", "Weight_in_lbs / 2.2")
  transformedTable2.show(5)

  // adding a column
  val transformedTable3 = carsDfWithSchema.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)
  transformedTable3.show(5)

  // renaming the column
  val transformedTable4 = carsDfWithSchema.withColumnRenamed("Weight_in_lbs", "weight (lbs)")
  transformedTable4.show()

  // using not usual column name not breaking expression compiler
  transformedTable4.selectExpr("`weight (lbs)`/2.2").show(5)

  // drop an column
  transformedTable4.drop("weight (lbs)", "Horsepower", "Year").show(2)


  /** FILTER OPS */

  // regular
  val notUSACars = carsDfWithSchema.filter(col("Origin") =!= "USA")
  val europeCars = carsDfWithSchema.filter(col("Origin") === "Europe")
  notUSACars.show(3)
  europeCars.show(3)

  // filtering with string expression
  val japanCars = carsDfWithSchema.filter("Origin = 'Japan'")
  japanCars.show(3)

  // chaining filters
  val japanPowerCars = carsDfWithSchema.filter((col("Origin") === "Japan").and(col("Horsepower") > 100))
  japanPowerCars.show(5)
  val japanPowerCars2 = carsDfWithSchema.filter("Origin = 'Japan' and Horsepower > 100")
  japanPowerCars2.show(5)

  // union
  val moreCarsDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/more_cars.json")
  moreCarsDf.show(2)
  val allCars = moreCarsDf.union(moreCarsDf)
  allCars.show(4)

  // distinct
  val allCountries = carsDfWithSchema.select("Origin").distinct()
  allCountries.show()
}

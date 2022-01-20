package databriks_course

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UDFs extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .getOrCreate()


  val salesPathSchema = StructType(Array(
    StructField("order_id", LongType),
    StructField("email", StringType),
    StructField("transaction_timestamp", LongType),
    StructField("total_item_quantity", LongType),
    StructField("purchase_revenue_in_usd", DoubleType),
    StructField("unique_items", LongType),
    StructField("items", StringType)
  ))

  val salesDF = spark
    .read
    .schema(salesPathSchema)
    .option("header", "true")
    .option("sep", ",")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/databriks_data/salesPath.csv")

  // salesPathDf.show()

  // define function
  def firstLetterFunction(email: String): String = {
    email(0).toString
  }

  /** Define a UDF that wraps the function. This serializes the function and sends it to executors to be able to use in our DataFrame. */
  val firstLetterUDF = udf(firstLetterFunction _)

  salesDF.select(firstLetterUDF(col("email")))
  //  .show()

  /** Register UDF using spark.udf.register to create UDF in the SQL namespace. */
  salesDF.createOrReplaceTempView("sales")

  spark.udf.register("sql_udf", firstLetterFunction _)

  spark.sql("select sql_udf(email) from sales")
  //  .show()

  /** udf lab */

  val start = spark
    .read
    .option("header", "true")
    .option("sep", ",")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/databriks_data/for_homework_with_udf.csv")

  def labelDayOfWeek(day: String): String = {
    day match {
      case "Mon" => "1-Mon"
      case "Tue" => "2-Tue"
      case "Wed" => "3-Wed"
      case "Thu" => "4-Thu"
      case "Fri" => "5-Fri"
      case "Sat" => "6-Sat"
      case "Sun" => "7-Sun"
      case _ => "UNKNOWN"
    }
  }

  val labelDowUDF = udf(labelDayOfWeek _)

  /** Update the day column by applying the UDF and replacing this column
   * Sort by day
   * Plot as bar graph */

  start.withColumn("day",labelDowUDF(col("day"))).orderBy("day").show()


}

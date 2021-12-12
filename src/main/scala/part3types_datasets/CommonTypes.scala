package part3types_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder().appName("Common Types").config("spark.master", "local").getOrCreate()
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  val r = new scala.util.Random //will not works

  // lit = Creates a Column of literal value
  moviesDF.select(col("Title"), lit("value will not change").as("literal_value"))

  // exercise: convert column to string and check it length
  moviesDF.selectExpr("Title", "length(Title) > 10", "current_date")

  // columns - booleans
  val beginsWithMyName = col("Title").startsWith("Art")
  moviesDF.filter(beginsWithMyName).select(col("Title"), beginsWithMyName)
  // negation
  moviesDF.filter(beginsWithMyName).select(col("Title"), not(beginsWithMyName)).show()

  // columns - numbers
  moviesDF.select((col("Worldwide_Gross")-col("Production_Budget")).as("[Worldwide_Gross] - [Production_Budget]"))

  /** STATISTIC: need to research someday **/
  // println(s"corr function example: ${moviesDF.stat.corr("Production_Budget","Worldwide_Gross")}")
  // Strings: initcap, length
  moviesDF.select(initcap(col("Title")),length(col("Title")))
  // contains
  moviesDF.filter(col("Title").contains("Art")).select(col("Title"))
  // regexp_extract operation
  val regExExample = "Arthur|Art"
  moviesDF.filter(col("Title").contains("Art")).select(col("Title"),regexp_extract(col("Title"),regExExample,0)).show()
}

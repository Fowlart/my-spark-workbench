package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  // countDistinct
  lazy val moviesCount = moviesDF.select(col("Major_Genre")).distinct().count()

  val sparkSession = SparkSession.builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = sparkSession
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // println(moviesCount)

  // count
  moviesDF.select(count(col("Major_Genre")))

  // count all rows in the table(count all json)
  moviesDF.select(count("*"))

  // countDistinct
  moviesDF.select(countDistinct("Major_Genre"))

  // quick count
  moviesDF.select(approx_count_distinct("Major_Genre"))

  // min/max/sum/avg/mean/stddev
  moviesDF.select(
    avg(col("US_Gross")),
    min(col("Worldwide_Gross")),
    max(col("US_Gross")),
    sum(col("US_Gross")),
    mean(col("Worldwide_Gross")),
    stddev(col("Rotten_Tomatoes_Rating")),
    countDistinct("Distributor")
  )

  moviesDF.selectExpr("avg(US_Gross)")

  // grouping
  // only one aggregation ?
  val usGrossForEachGenre = moviesDF.groupBy(col("Major_Genre")).sum("US_Gross")
  val groupedByRating = moviesDF.groupBy(col("MPAA_Rating").as("Distinct rating groups")).count()
  groupedByRating.show()



}

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

  /** Here are some of the built-in functions available for aggregation, without groupBy call.
   * 'import org.apache.spark.sql.functions._' required. */
  moviesDF.select(
    avg(col("US_Gross")),
    min(col("Worldwide_Gross")),
    max(col("US_Gross")),
    sum(col("US_Gross")),
    mean(col("Worldwide_Gross")),
    stddev(col("Rotten_Tomatoes_Rating")),
    countDistinct("Distributor"),
    corr("Worldwide_Gross", "US_Gross"),
    collect_set("Distributor")
  )

  moviesDF.selectExpr("avg(US_Gross)")

  /** Aggregate built-in functions. GroupBy returns RelationalGroupedDataset. */
  val sumUsGrossForEachGenre = moviesDF.groupBy(col("Major_Genre")).sum("US_Gross")
  val maxGrossForEachGenre = moviesDF.groupBy(col("Major_Genre")).max("US_Gross")
  val avgGrossForEachGenre = moviesDF.groupBy(col("Major_Genre")).avg("US_Gross")
  // ???
  val x = moviesDF.groupBy(col("Major_Genre")).pivot("US_Gross")

  // Will show a sum by each Major_Genre group(and 'Major_Genre' values as well)
  // sumUsGrossForEachGenre.show()

  // Grouping by multiply columns:
  moviesDF
    .groupBy(col("Major_Genre"), col("Distributor"))
    .avg("US_Gross")
    .orderBy(col("Major_Genre").desc)
    .show()

  // Applying function above groupBy to specify aggregation
  // Apply multiple aggregate functions on grouped data
  moviesDF
    .groupBy(col("Major_Genre"), col("Distributor"))
    .agg(sum("US_Gross").as("sum of US_Gross"), avg("US_Gross").as("avg of US_Gross"))
}

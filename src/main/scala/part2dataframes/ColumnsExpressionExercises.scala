package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr, when}

object ColumnsExpressionExercises extends App {

  /** Task:
    * 1 - read the movies DF, select 2 columns;
    * 2 - create column: profit sum;
    * 3 - select all comedy with IMDB > 6;
    * 4 - use many different ways. */

  val spark = SparkSession.builder().appName("Data Sources").config("spark.master", "local").getOrCreate()

  val carsDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("nullValue", 0)
    .load("src/main/resources/data/movies.json")

  carsDf.select("Title", "Major_Genre")
  carsDf.select(col("Title"), col("Major_Genre"))
  carsDf.select(column("Title"), column("Major_Genre"))

  import spark.implicits._

  println("==> TABLES WITH IMPLICIT USAGE <==")
  carsDf.select('Title, 'Major_Genre)
  carsDf.select($"Title", $"Major_Genre")

  println("==> CREATE COLUMN WITH SUM OF PROFITS COLUMNS <==")

  // not so smart
  carsDf.filter(col("US_DVD_Sales").isNotNull).select(column("Title"), col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))

  // smarter:
  carsDf.select(column("Title"),
    col("US_Gross") + col("Worldwide_Gross") +
      when(col("US_DVD_Sales").isNull, 0).when(col("US_DVD_Sales").isNotNull, col("US_DVD_Sales")))

  carsDf.select(col("Title"),
    expr("((US_Gross + Worldwide_Gross) + CASE WHEN (US_DVD_Sales IS NULL) THEN 0 WHEN (US_DVD_Sales IS NOT NULL) THEN US_DVD_Sales END)")
      .as("SUM OF SALES"))

  // more smarter
  carsDf.select(col("Title"),
    expr("(US_Gross + Worldwide_Gross) + coalesce(US_DVD_Sales,0)")
      .as("SUM OF SALES")).show(5)

  carsDf.selectExpr("Title", "US_Gross + Worldwide_Gross + coalesce(US_DVD_Sales,0)")

  println("==> select all comedy with IMDB more than 6 <==")
  carsDf.filter("Major_Genre like 'Comedy' AND IMDB_Rating > 6")
  carsDf.filter(col("Major_Genre").eqNullSafe("Comedy").and(col("IMDB_Rating") > 6))
  carsDf.select("Title").where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show(5)


}

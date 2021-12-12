package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val movies = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // count all rows
  // count, col form => org.apache.spark.sql.functions._
  val genresCountDf = movies.select(count(col("Major_Genre")))
  movies.selectExpr("count(Major_Genre)") // excludes nulls
  movies.select(count("*")) // not includes nulls

  //distinct values; countDistinct => org.apache.spark.sql.functions._
  val genresCountDfDistinct = movies.select(countDistinct(col("Major_Genre")))

  // approximate(will work quicker). Useful for big bigger data amount.
  movies.select(approx_count_distinct(col("Major_Genre")))

  //min and max
  movies.select(min(col("IMDB_Votes")))
  movies.selectExpr("min(IMDB_Votes)")

  //sum
  movies.select(sum(col("Worldwide_Gross")))
  movies.selectExpr("sum(Worldwide_Gross)")

  //avg
  movies.select(avg(col("Worldwide_Gross")))
  movies.selectExpr("avg(Worldwide_Gross)")

  //data science
  movies.select(mean(col("Worldwide_Gross")), stddev(col("Worldwide_Gross")))

  //groupBy
  movies.groupBy(col("Major_Genre")).count()

  movies.groupBy(col("Major_Genre")).avg("IMDB_Votes")

  movies.groupBy(col("Major_Genre"))
    .agg(count("*").as("movies count"), avg("IMDB_Votes").as("avg IMDB_Votes"))
    .orderBy("movies count")

  /** Tasks:
    * 1 - sum of all the profits of all movies
    * 2 - how many distinct directors we have
    * 3 - mean and standard deviation of US gross revenue
    * 4 - compute the avg IMDB, avg US gross by director */

  //1 -sum of all the profits of all movies
  // why they are different ???
  movies.select(( sum(col("Worldwide_Gross")) + sum(col("US_Gross")) + sum(col("US_DVD_Sales")) ).as("SUM")).show()

  movies.select((col("Worldwide_Gross") + col("US_Gross") + col("US_DVD_Sales")).as("SUM")).select(sum("SUM")).show()

  //2 - how many distinct directors we have
  movies.select(countDistinct(col("Director")).as("how many distinct directors we have"))

  //3 - mean and standard deviation of US gross revenue
  movies.select(mean(col("US_Gross")).as("mean"), stddev(col("US_Gross")).as("standard deviation"))
  movies.select(avg(col("US_Gross")).as("avg()"))

  //4 - compute the avg IMDB, avg US gross by director
  movies.groupBy("Director").agg(avg("IMDB_Votes").as("avg IMDB_Votes"),
    avg("US_Gross").as("avg US_Gross"))


}

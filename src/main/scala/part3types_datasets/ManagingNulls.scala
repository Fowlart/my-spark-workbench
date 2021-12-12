package part3types_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.language.postfixOps

object ManagingNulls extends App {

  val spark = SparkSession.builder().appName("Managing Nulls").config("spark.master", "local").getOrCreate()
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // coalesce usage
  val rtr = col("Rotten_Tomatoes_Rating")
  val imdb = col("IMDB_Rating")
  val title = col("Title")
  moviesDF.select(title, coalesce(rtr, imdb))

  // filter and finding nulls
  moviesDF.filter(imdb isNull).select(title, imdb)

  // orderBy based on null values
  moviesDF.select(imdb).orderBy(imdb.desc_nulls_first)

  /** na - object */
  //remove nulls
  moviesDF.select(imdb).na.drop().show()

  // replace nulls by fill
  moviesDF.na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating"))
    .select(title, rtr, imdb)
    .show()
  // more powerful fill
  moviesDF.na.fill(Map("Rotten_Tomatoes_Rating" -> 0, "Director" -> "unknown"))
    .select("Rotten_Tomatoes_Rating", "Director")
    .show(20)

}

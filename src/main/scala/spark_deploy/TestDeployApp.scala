package spark_deploy

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object TestDeployApp {
  /** We need main method for accept command-line arguments */
  def main(args: Array[String]): Unit = {

    /** Movies.json - arg 0 */
    if (args.length !=2) {
      println("Need correct args quantities")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Test Deploy App")
      .config("spark.master", "local")
      .getOrCreate

    val moviesDF = spark
      .read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComedies = moviesDF.select(col("IMDB_Rating") > 7)

    goodComedies
      .write
      .mode(SaveMode.Overwrite)
      .json(args(1))
  }
}

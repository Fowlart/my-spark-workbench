package big_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AwsTaxiApp {

  def main(args: Array[String]): Unit = {

    val yellowTripDataRemotePath = args(0)
    val pathToSaveResult = args(1)

    if (args.length != 2) System.exit(1)

    val spark = SparkSession.builder()
      .appName("AwsTaxiApp")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiDf = spark
      .read
      .parquet(yellowTripDataRemotePath)

    val focusOnTripDist = taxiDf.select("trip_distance")

    val tripDuration = spark.range(0, 7656, 4).withColumnRenamed("id", "tripDuration")
    val joinCondition = (taxiDf.col("trip_distance") < tripDuration.col("tripDuration")) && (taxiDf.col("trip_distance") > (tripDuration.col("tripDuration") - 4))
    val joinedWithDuration = tripDuration.join(focusOnTripDist, joinCondition, "inner")
    val result = joinedWithDuration.groupBy(col("tripDuration")).count().orderBy(col("tripDuration").desc_nulls_first)

    result
      .write
      .option("header", "true")
      .csv(pathToSaveResult)
  }
}

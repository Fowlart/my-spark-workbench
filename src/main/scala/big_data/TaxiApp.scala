package big_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxiApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TaxiApp")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiDf = spark
      .read
      .parquet("src/main/resources/data/yellow_tripdata_2018-07")

    val zones = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/data/taxi+_zone_lookup.csv")

    zones.printSchema()
    taxiDf.printSchema()

    /** Which of zones have most of pick-ups and drop-of */
    val maxByPULocationID = taxiDf.groupBy("PULocationID").agg(count("*").as("total_rides")).join(zones, col("LocationID") === col("PULocationID")).orderBy(col("total_rides").desc_nulls_last).select("total_rides", "Zone")
    val maxByDOLocationID = taxiDf.groupBy("DOLocationID").count().join(zones, col("LocationID") === col("DOLocationID")).orderBy(col("count").desc_nulls_last).select("count", "Zone")
    // maxByPULocationID.show(1)
    // maxByDOLocationID.show(1)

    /** Which are peak hours for taxi */
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val hours_CountOfPassengers_SortedByCount = taxiDf.select(hour(col("tpep_pickup_datetime")).as("hour"),
      col("passenger_count")).groupBy("hour").count().orderBy(col("count").desc_nulls_last)
    // hours_CountOfPassengers_SortedByCount.show()

    /** How the trips distributed by length */
    taxiDf.select(stddev("trip_distance"),
      min("trip_distance"),
      max("trip_distance"),
      mean("trip_distance"))

    val focusOnTripDist = taxiDf.select("trip_distance")

    val tripDuration = spark.range(0, 7656, 4).withColumnRenamed("id", "tripDuration")
    val joinCondition = (taxiDf.col("trip_distance") < tripDuration.col("tripDuration")) && (taxiDf.col("trip_distance") > (tripDuration.col("tripDuration") - 4))
    val joinedWithDuration = tripDuration.join(focusOnTripDist, joinCondition, "inner")
    joinedWithDuration.groupBy(col("tripDuration")).count().orderBy(col("tripDuration").desc_nulls_first).show(100)
  }
}
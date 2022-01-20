package databriks_course

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Datetimes extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .getOrCreate()


  // Let's use a subset of the BedBricks events dataset to practice working with date times.
  val datetimes = spark
    .read
    .option("header", "true")
    .option("sep", ",")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/databriks_data/date_times.csv")

  // cast() - casts column to a different data type, specified using string representation or DataType.
  val timestampDF = datetimes.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))

  // date_format() - Converts a date/timestamp/string to a string formatted with the given date time pattern.
  val formattedDF = timestampDF
    .withColumn("date string", date_format(col("timestamp"), "MMMM dd, yyyy"))
    .withColumn("time string", date_format(col("timestamp"), "HH:mm:ss"))

  // extract datetime attribute from timestamp
  val datetimeDF = timestampDF
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("dayofweek", dayofweek(col("timestamp")))
    .withColumn("minute", minute(col("timestamp")))
    .withColumn("second", second(col("timestamp")))

  // to_date - Convert to Date
  val dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))

  // date_add - Returns the date that is the given number of days after start
  val plus2DF = timestampDF.withColumn("plus_two_days", date_add(col("timestamp"), 2))

  /** Get daily active users
   * Group by date
   * Aggregate approximate count of distinct user_id and alias with "active_users"
   * Recall built-in function to get approximate count distinct
   * Sort by date
   * Plot as line graph */

  val start = spark
    .read
    .option("header", "true")
    .option("sep", ",")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/databriks_data/date_time_1.csv")

  val activeUsersDF = start.groupBy(col("date"))
    .agg(approx_count_distinct(col("user_id")).as("active_users"))
    .orderBy(col("date"))

  /** Get average number of active users by day of week
   * Add day column by extracting day of week from date using a datetime pattern string
   * Group by day
   * Aggregate average of active_users and alias with "avg_users" */
  val activeDowDF = activeUsersDF
    .withColumn("day", date_format(col("date"), "E"))
    .groupBy("day")
    .agg(avg(col("active_users")).as("avg_users"))

  activeDowDF.show()


}

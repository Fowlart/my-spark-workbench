package part4spark_sql

import org.apache.spark.sql.{SparkSession}

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .getOrCreate()

  val cars = spark
    .read
    .json("src/main/resources/data/cars.json")

  cars.createOrReplaceTempView("cars") // will create table on operation memory

  // using plain SQL
  spark.sql(
    """select Origin as origin, avg(Acceleration) as avg_acceleration
      |from cars
      |group by Origin""".stripMargin).show()

}

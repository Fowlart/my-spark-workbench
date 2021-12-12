package spark_deploy

import org.apache.spark.sql.SparkSession

object ConvertTaxiFile extends App {

  val spark = SparkSession.builder()
    .appName("ConvertTaxiFile")
    .config("spark.master", "local")
    .getOrCreate()

  val newTaxiDf = spark
    .read
    .parquet("src/main/resources/data/yellow_tripdata_2018-07")

  newTaxiDf.printSchema

  newTaxiDf
    .show()
}

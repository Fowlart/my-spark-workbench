package lab

import org.apache.spark.sql.SparkSession

object AptosLab extends App {

  val spark = SparkSession.builder()
    .appName("AptosLab")
    .config("spark.master", "local")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()

  val aptos_daily_sales_stage = spark.read
    .option("compression", "snappy")
    .parquet("/Users/artur/Desktop/Tasks/7637/aptos_daily_sales_stage")

  val aptos_promotions_stage = spark.read
    .option("compression", "snappy")
    .parquet("/Users/artur/Desktop/Tasks/7637/aptos_promotions_stage")

  val exchange_rate_aptos = spark.read
    .format("delta")
    .load("/Users/artur/Desktop/Tasks/7637/exchange_rate_aptos")

  val sa_mcs_salhist = spark.read
    .format("delta")
    .load("/Users/artur/Desktop/Tasks/7637/sa_mcs_salhist")

  // aptos_promotions_stage.show()
   aptos_daily_sales_stage.show(10)
  // exchange_rate_aptos.show()
  // sa_mcs_salhist.show(10)
}

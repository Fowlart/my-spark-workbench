package lab

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, coalesce, col, current_date, date_format, date_trunc, desc, length, lit, row_number, to_date, when}
import org.apache.spark.sql.types.StringType

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

  import spark.implicits._

  aptos_daily_sales_stage.select(date_format(col("BUSINESS_DATE"),"MM")).show(100)


}

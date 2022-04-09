package to_kafka

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToAvroConverter extends App {

  val spark = SparkSession
    .builder()
    .appName("CsvToAvroConverter")
    .config("spark.master", "local")
    .getOrCreate()

  def convert(csvInputPath: String, csvOutputPath: String): Unit = {
    spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("nullValue", "")
      .load(csvInputPath)
      .write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(csvOutputPath)
  }

  convert(
    "src/main/resources/spark_to_scala/loyalty_account.csv",
    "src/main/resources/spark_to_scala/avro/loyalty_account"
  )

  convert(
    "src/main/resources/spark_to_scala/order_header_consolidated.csv",
    "src/main/resources/spark_to_scala/avro/order_header_consolidated"
  )

  convert(
    "src/main/resources/spark_to_scala/preaudit_line_sales.csv",
    "src/main/resources/spark_to_scala/avro/preaudit_line_sales"
  )

  convert(
    "src/main/resources/spark_to_scala/order_line_consolidated.csv",
    "src/main/resources/spark_to_scala/avro/order_line_consolidated"
  )
}

package to_kafka
import org.apache.spark.sql.SparkSession

import java.util.{List => _}

object ToKafkaApp extends App {

  val spark = SparkSession
    .builder()
    .appName("AptosLab")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def fillLoyaltyAccount() = {

    val loyaltyAccount =
      spark.read
        .format("csv")
        .option("sep", ",")
        .option("header", "true")
        .option("nullValue", "")
        .load("src/main/resources/spark_to_scala/loyalty_account.csv")

    loyaltyAccount.select(
      "related_loyalty_usa_id",
      "signup_channel",
      "first_name",
      "account_create_date")
      .show()

    val bootstrapServers = "localhost:9092"
    val outputTopic = "loyaltyAccount"

    loyaltyAccount.selectExpr("CAST(account_create_date AS STRING) as value")
      .write
      .format("kafka")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("startingOffsets", "earliest")
      .option("topic", outputTopic)
      .option("failOnDataLoss", "false")
      .save()
  }


  def fillOrderHeaderConsolidated() = {

    val orderHeaderConsolidated =
      spark.read
        .format("csv")
        .option("sep", ",")
        .option("header", "true")
        .option("nullValue", "")
        .load("src/main/resources/spark_to_scala/order_header_consolidated.csv")

    orderHeaderConsolidated.show()

    val bootstrapServers = "localhost:9092"
    val outputTopic = "orderHeaderConsolidated"

    orderHeaderConsolidated.selectExpr("CAST(atg_id AS STRING) as value")
      .write
      .format("kafka")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("startingOffsets", "earliest")
      .option("topic", outputTopic)
      .option("failOnDataLoss", "false")
      .save()
  }

  fillOrderHeaderConsolidated()

}
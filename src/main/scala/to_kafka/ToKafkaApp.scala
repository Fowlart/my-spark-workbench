package to_kafka
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.{List => _}

object ToKafkaApp extends App {

  val spark = SparkSession
    .builder()
    .appName("ToKafkaApp")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bootstrapServers = "localhost:9092"

  val loyaltyAccount = spark.read.format("avro").load("src/main/resources/spark_to_scala/avro/loyalty_account")

  val orderHeaderConsolidated =
    spark.read.format("avro").load("src/main/resources/spark_to_scala/avro/order_header_consolidated")

  val preAuditLineSales = spark.read.format("avro").load("src/main/resources/spark_to_scala/avro/preaudit_line_sales")

  val orderLineConsolidated =
    spark.read.format("avro").load("src/main/resources/spark_to_scala/avro/order_line_consolidated")


  def transferDfAndLoad(df: DataFrame, topicName: String) = {

    // df.select(to_avro(struct("*"))).show(false)

    df
      .select(to_avro(struct("*")) as "value")
      .write
      .format("kafka")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("startingOffsets", "earliest")
      .option("topic", topicName)
      .option("failOnDataLoss", "false")
      .save()
  }

  transferDfAndLoad(loyaltyAccount, "loyaltyAccount")
  // transferDfAndLoad(orderHeaderConsolidated, "orderHeaderConsolidated")
  // transferDfAndLoad(orderLineConsolidated, "orderLineConsolidated")
  // transferDfAndLoad(preAuditLineSales, "preAuditLineSales")
}

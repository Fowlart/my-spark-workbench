package to_kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object TreSparkKafkaConsumer {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("FromLocalToKafka")
    .getOrCreate()

  val sc = spark.sparkContext
  spark.conf.set("spark.sql.shuffle.partitions", "20")

  //  val schemaRegistry = "http://0.0.0.0:8081"

  val demandOrderTenderAvroSchema =
    org.apache.spark.sql.avro.SchemaConverters.toAvroType(
      spark.read.format("delta").load("Data/Tre/demand_order_tender").schema
    )
  println(demandOrderTenderAvroSchema.toString(true))

  //  val orderHeaderConsolidatedAvroSchema =
  //    org.apache.spark.sql.avro.SchemaConverters.toAvroType(
  //      spark.read
  //        .format("delta")
  //        .load("Data/Tre/order_header_consolidated")
  //        .schema
  //    )
  //  println(orderHeaderConsolidatedAvroSchema.toString(true))

  val demandOrderTender = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9090")
    .option("subscribe", "Sephora.DataPlatform.TRE.OrderTenderDetailsEvents-changelog")
    .option("startingOffsets", "earliest") // From starting
    .load()

  demandOrderTender.select(from_avro(col("value"), demandOrderTenderAvroSchema.toString()).as("data"))
    .select("data.*")
    .show(false)

}
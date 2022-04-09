package to_kafka
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions.struct

object TreSparkKafkaProducer {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("FromLocalToKafka")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.conf.set("spark.sql.shuffle.partitions", "20")

  //  val schemaRegistry = "http://0.0.0.0:8081"

  val demandOrderTender = spark.read.format("delta").load("Data/Tre/demand_order_tender")
  val demandOrderTenderAvroSchema = org.apache.spark.sql.avro.SchemaConverters.toAvroType(demandOrderTender.schema)
  println(demandOrderTenderAvroSchema.toString(true))

  val orderHeaderConsolidated = spark.read.format("delta").load("Data/Tre/order_header_consolidated")
  val orderHeaderConsolidatedAvroSchema = org.apache.spark.sql.avro.SchemaConverters.toAvroType(orderHeaderConsolidated.schema)
  println(orderHeaderConsolidatedAvroSchema.toString(true))


  demandOrderTender.select(to_avro(struct("*")) as "value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9090")
    .option("topic", "Sephora.DataPlatform.TRE.OrderTenderDetailsEvents-changelog")
    .save()

  orderHeaderConsolidated.select(to_avro(struct("*")) as "value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9090")
    .option("topic", "Sephora.DataPlatform.TRE.OrderHeaderConsolidated-changelog")
    .option("checkpointLocation","checkpoints/order_header_consolidated")
    .save()

}

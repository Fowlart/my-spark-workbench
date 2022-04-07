package databriks_course
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lower, regexp_replace}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object SimpleStream extends App {

  val spark = SparkSession.builder().appName("Joins").config("spark.master", "local").getOrCreate()

  val personSchema = StructType(Array(
    StructField("firstName", StringType),
    StructField("middleName", StringType),
    StructField("lastName", StringType),
    StructField("birthDate", StringType),
    StructField("salary", StringType),
    StructField("ssn", StringType)))

  val df = spark
    .readStream
    .schema(personSchema)
    .option("maxFilesPerTrigger", 1)
    .option("sep", ":")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/databriks_data/streaming/input")

  println(df.isStreaming)

  val transformedDf = df.select(col("firstName")).distinct()

  val devicesQuery = transformedDf
    .writeStream
    .outputMode("append")
    .format("parquet")
    .queryName("email_traffic_s")
    .trigger(Trigger.ProcessingTime("1 second"))
    .option("checkpointLocation", "src/main/resources/databriks_data/streaming/checkpoint")
    .start("src/main/resources/databriks_data/streaming/output")


}

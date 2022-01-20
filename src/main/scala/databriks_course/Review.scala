package databriks_course

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Review extends App{

  val spark = SparkSession.builder().appName("Joins").config("spark.master", "local").getOrCreate()

  val persons = spark
    .read
    .option("inferSchema","true")
    .option("sep", ":")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/databriks_data/persons.csv")

  persons
    .withColumn("firstName",initcap(lower(col("firstName"))))
    .withColumn("middleName",initcap(lower(col("middleName"))))
    .withColumn("lastName",initcap(lower(col("lastName"))))
    .withColumn("ssn",regexp_replace(col("ssn"),"-",""))
    .distinct()
    .repartition(8)
    .write
    .parquet("src/main/resources/databriks_data/persons")
}

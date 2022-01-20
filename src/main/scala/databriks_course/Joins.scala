package databriks_course

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object Joins extends App{

  val spark = SparkSession.builder().appName("Joins").config("spark.master", "local").getOrCreate()
  val guitarPlayers = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val guitars = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val bands = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  guitarPlayers.select(col("band"),lit(true).as("converted")).show()

  val joinCondition = guitarPlayers.col("band") === bands.col("id")

  guitarPlayers.join(bands, joinCondition, "outer")
    .filter(col("band").isNotNull)
    .na.fill(0,Seq("guitars"))

  guitars.groupBy(col("gType")).agg(collect_set(col("id")).alias("collected_ids")).show()

}

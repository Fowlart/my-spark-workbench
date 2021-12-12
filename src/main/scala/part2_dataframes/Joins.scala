package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Joins extends App {

  val spark = SparkSession.builder().appName("Joins").config("spark.master", "local").getOrCreate()
  val guitarPlayers = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val guitars = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val bands = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  // inner - is join type by default
  // simple join based on the same column
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitarPlayersWithBands = guitarPlayers.join(bands, joinCondition, "inner")
  val allGuitarPlayersBands = guitarPlayers.join(bands, joinCondition, "left_outer")
  val allBandsWithGuitarPlayers = bands.join(guitarPlayers, joinCondition, "left_outer")
  val allGuitarPlayersAllBands = guitarPlayers.join(bands, joinCondition, "outer")
  val onlyGuitarPlayersWhereBandsExists = guitarPlayers.join(bands, joinCondition, "left_semi")
  val onlyBandsWhereGuitarPlayersExists = bands.join(guitarPlayers, joinCondition, "left_semi")
  val onlyGuitarPlayersWhereBandsDONtExists = guitarPlayers.join(bands, joinCondition, "left_anti")

 // guitarPlayersWithBands.select(col("id")).show() // will crash because of 2 'id' column

  val modifiedBand = bands.withColumnRenamed("id","band_id")
  guitarPlayers.join(modifiedBand, guitarPlayers.col("band") === modifiedBand.col("band_id"), "inner").select(col("id")).show()
}

package part3types_datasets

import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object DataSets extends App {

  val spark = SparkSession.builder().appName("Data Sets").config("spark.master", "local").getOrCreate()

  val numbers = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbers.printSchema()

  // converting df column as data-set
  val intEncoder = Encoders.scalaInt
  val intDataset: Dataset[Int] = numbers.select("numbers").as[Int](intEncoder)

  // dataset -> array
  println(intDataset.collect().mkString("[", ", ", "]"))
  // 3 - convert to dataset

  import spark.implicits._

  val carsDataSet = readJson("cars.json")
    .as[Car]

  // dataset of complex type:
  /** Exercises */
  // count numbers of cars:
  val carsNumber = carsDataSet.count

  // 2 - define an encoder(importing implicits)

  // count number of powerful car
  val countOfPowerfulCar = carsDataSet.filter(car => car.Horsepower.getOrElse(0L) > 140).count
  // avg horsePower
  val avgHorsePower = carsDataSet.map(car => car.Horsepower.getOrElse(0L)).reduce(_ + _) / carsNumber

  def readJson(fileName: String) =
    spark.read
      .format("json")
      .option("inferSchema", "true")
      .option("ignoreNullFields", "true")
      .json(s"src/main/resources/data/$fileName")

  // 1 - need to define case class in order to represent complex type
  case class Car(Name: String,
                 Miles_per_Gallon: Option[Double],
                 Cylinders: Option[Long],
                 Displacement: Option[Double],
                 Horsepower: Option[Long],
                 Weight_in_lbs: Option[Long],
                 Acceleration: Option[Double],
                 Year: String,
                 Origin: String) {

    override def toString: String = {
      s"[$Name] - [$Year]"
    }
  }

  println(s"carsNumber: $carsNumber")
  println(s"countOfPowerfulCar: $countOfPowerfulCar")
  println(s"avgHorsePower: $avgHorsePower")
  carsDataSet.select(avg("Horsepower"))

 // carsDataSet.collectAsList().forEach(car=>println(car))

  // Dataframe is Dataset[Row]
  // Joins
  case class Guitar(id: Long,
                    model: String,
                    make: String,
                    gType: String) {
    override def toString: String = {
      s"[$model] - [$gType]"
    }
  }

  case class GuitarPlayer(id: Long,
                          name: String,
                          guitars: Seq[Long],
                          band: Long){
    override def toString: String = {
      s"[$name] - [$band]"
    }
  }

  case class Band(id: Long, name: String, homeTown: String, year: BigInt){
    override def toString: String = {
      s"[$name] - [$homeTown]"
    }
  }

  val guitars = readJson("guitars.json")
    .as[Guitar]

  val guitarPlayers =  readJson("guitarPlayers.json")
   .as[GuitarPlayer]

  val band = readJson("bands.json")
    .as[Band]

  // joining datasets give us a tuple of datasets(not a dataframe)
  val result: Dataset[(Band,GuitarPlayer)] = band.joinWith(guitarPlayers, guitarPlayers.col("band")===band.col("id"),"inner")

  // Join guitarPlayers and guitars
  val guitarPlayerGuitar: Dataset[(GuitarPlayer,Guitar)] = guitarPlayers.joinWith(guitars,
    array_contains(guitarPlayers.col("guitars"),
      guitars.col("id")),"right_outer")
  guitarPlayerGuitar.collectAsList().forEach(tup=>println(s"${tup._1} <=> ${tup._2}"))

  //groupBy related class field
  carsDataSet
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations - time



}

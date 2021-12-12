package rrd_s

import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Rdds extends App {

  val spark = SparkSession.builder()
    .appName("Rdds")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  /** Way to create rrds */

  // parallelize existed collection:
  val collectOfNumbers = 1 to 10000
  sc.parallelize(collectOfNumbers)

  //converting from ds
  val fuser = spark
    .read
    .load("spark-warehouse/fuser")

  println(fuser.schema)

  case class Fuser(user_id: Long, email: String) {
    override def toString: String = s"$user_id: $email"
  }

  import spark.implicits._

  val fuserRDD = fuser.as[Fuser].rdd

  // fuserRDD.foreach(rdd => println(rdd))

  // rdd
  val rddWithoutTypeInfo = fuser.rdd

  //converting BACK rdd to df
  //  rddWithoutTypeInfo.toDF() - will not work
  // fuserRDD.toDF("user_id", "email", "active").show()

  /** RDDs ACTIONS */
  /** transformation: */

  // filtering rdd with type info
  fuserRDD.filter(fu => fu.email.contains("fowl")).toDF().show()

  // filtering rdd without type info
  val filteredRow = rddWithoutTypeInfo.filter(row => row.getString(2).contains("fowl")).first()
  println(filteredRow)

  // max, rdd with type info
  implicit val fuserOrdering: Ordering[Fuser] = Ordering.fromLessThan((f1, f2) => f1.user_id < f2.user_id)

  println(s"Max user id is ${fuserRDD.max().user_id}")

  // reducing
  val reducedValue = collectOfNumbers.sum
  println(s"reducing example > $reducedValue")

  //grouping [expensive operation]
  val groupedFuser = fuserRDD.groupBy(f => f.email.contains("fowl"))

  // repartitioning [expensive operation]
  val repartitioned = fuserRDD.repartition(30)

  // writing to file
  repartitioned
    .toDF
    .write
    .mode(SaveMode.Overwrite)
    .parquet("spark-warehouse/fuser30")

  /** The best way to make a partition is keep each file size between 10-100MB * */

  // coalesceThePartitions
  val coalesced = repartitioned.coalesce(2)

  coalesced
    .toDF
    .write
    .mode(SaveMode.Overwrite)
    .parquet("spark-warehouse/fuser2")

  /** task #1: read Movie as rdd * */
  case class Movie(title: String, rating: Double) {
    override def toString: String = s"[$title - $rating]"
  }

  val moviesJsonDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  val moviesRdd = moviesJsonDf
    .na
    .fill(0, List("IMDB_Rating"))
    .select(col("IMDB_Rating").as("rating"), col("Title").as("title"))
    .as[Movie]
    .rdd

  /** task #2: make an advance filtering */
  moviesRdd.filter(mov => mov.rating > 7).toDF().show()

  /** task #3: terminal ops */
  val avgRating = moviesRdd.map(mov => mov.rating).sum / moviesRdd.count
  println(s"average rating: $avgRating")

  /** task #4: avg by grouping by */
  val avgDf = moviesRdd.toDF().select(avg("rating"))
  avgDf.show()


}
package rdd

import org.apache.spark.sql.SparkSession

object MapVsFlatMap extends App {

  val sparkSession = SparkSession.builder()
    .appName("MapVsFlatMap")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val rdd = sc.parallelize(Seq("Roses are red", "Violets are blue"))

  println(rdd.map(sentence => sentence.length).collect().mkString("[", ", ", "]"))

  /** FlatMap transforms an RDD of length N into a collection of N collections,
   * then flattens these into a single RDD of results. */
  println(rdd.flatMap(sentence => sentence.split(" ")).collect().mkString("[", ", ", "]"))
}

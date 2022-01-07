package rdd.transformations

import org.apache.spark.sql.SparkSession

object RDDsWideTransformation extends App {

  val sparkSession = SparkSession.builder()
    .appName("RDDsWideTransformation")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  /** =Wide transformations:=
   * input from other partitions are required. Data shuffling is needed before processing
   */

  // intersectionAndDistinct()
  // repartition()
  // coalesce()

  def intersectionAndDistinct(): Unit = {
    val x = sc.parallelize(1 to 5)
    val y = sc.parallelize(3 to 6, 2)
    val intersected = x.intersection(y).collect()
    println("\n Intersection: \n")
    println(intersected.mkString("[", ", ", "]"))
    println("\n Distinct: \n")
    val xx = sc.parallelize(Array(1, 2, 2, 2, 3))
    val distinctCollection = xx.distinct().collect()
    println(distinctCollection.mkString("[", ", ", "]"))
  }

  /** Keep in mind that repartitioning your data is a fairly expensive operation.
   * Spark also has an optimized version of repartition() called coalesce() that allows
   * avoiding data movement, but only if you are decreasing the number of RDD partitions. */
  def coalesce(): Unit = {
    println("\n Coalesce(): \n")
    val x = sc.parallelize(1 to 10, 5)
    val toPrint_1 = x.mapPartitions(iter => Seq(s"""partition: ${iter.mkString("[", ", ", "]")}""").iterator).collect()
    println(s"Initial rdd: ${toPrint_1.mkString("[", ", ", "]")}")
    val coalesce = x.coalesce(2)
    val toPrint_2 = coalesce.mapPartitions(iter => Seq(s"partition: ${iter.mkString("[", ", ", "]")}").iterator).collect()
    println(s"Coalesced rdd: ${toPrint_2.mkString("[", ", ", "]")}")
  }

  def repartition(): Unit = {
    println("\n Repartition(): \n")
    val x = sc.parallelize(1 to 10, 5)
    val toPrint_1 = x.mapPartitions(iter => Seq(s"""partition: ${iter.mkString("", ", ", "")}""").iterator).collect()
    println(s"${toPrint_1.mkString("", "; ", "")}")
    val coalesce = x.repartition(2)
    val toPrint_2 = coalesce.mapPartitions(iter => Seq(s"partition: ${iter.mkString("", ", ", "")}").iterator).collect()
    println(s"${toPrint_2.mkString("", "; ", "")}")
  }
}

package rdd

import org.apache.spark.sql.SparkSession

object NarrowWideTransformation extends App {

  val sparkSession = SparkSession.builder()
    .appName("NarrowWideTransformation")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  /** =Narrow Transformations:=
   * Output and output stays in the same partition
   *
   * No data movement is needed
   * */
  // Map vs Flatmap
  val collection_flat_map = sc.parallelize(1 to 10).flatMap(x => Seq(x, x))
  val collection_map = sc.parallelize(1 to 10).map(x => Seq(x, x))
  println("\n FlatMap: \n")
  collection_flat_map.foreach(it => print(s" $it"))
  println("\n Map: \n")
  collection_map.foreach(it => print(s" $it"))

  // MapPartitions ???
  println("\n MapPartitions: \n")
  val rangeWith5Slice = sc.parallelize(1 to 10, 10)
  val mapPartitionsFromRange = rangeWith5Slice.mapPartitions(x => Seq(s"${x.next()} mapped").iterator)
  mapPartitionsFromRange.foreach(it => print(s" $it"))

  // Zipping: like union, but elements are allowed to be of the different type
  println("\n Zipping: \n")
  val chars = sc.parallelize('A' to 'F')
  val numbers = sc.parallelize(1 to 6)
  val zippedCollection = chars.zip(numbers)
  zippedCollection.foreach(it => print(s" [${it._1} - ${it._2}] "))
  println()

  // Union
  println("\n Union: \n")
  val moreNumbers = sc.parallelize(7 to 10)
  val joinedCollection = numbers.union(moreNumbers)
  joinedCollection.foreach(num => print(s"$num|"))

  /** =Wide transformations:=
   * Input from other partitions are required
   * Data shuffling is needed before processing
   */
  def intersectionAndDistinct(): Unit = {
    val x = sc.parallelize(1 to 5)
    val y = sc.parallelize(3 to 6, 2)
    val intersected = x.intersection(y)
    println("\n Intersection: \n")
    intersected.foreach(num => print(s" $num "))
    println()
    println("\n Distinct: \n")
    val xx = sc.parallelize(Array(1, 2, 1, 3, 2))
    val distinctCollection = xx.distinct()
    distinctCollection.foreach(num => print(s" $num "))
  }

  /** Keep in mind that repartitioning your data is a fairly expensive operation.
   * Spark also has an optimized version of repartition() called coalesce() that allows
   * avoiding data movement, but only if you are decreasing the number of RDD partitions. */
  def coalesce(): Unit = {
    println("\n coalesce(): \n")
    val x = sc.parallelize(1 to 10, 5)
    x.foreachPartition(part => {
      println(" \npartition: ")
      part.foreach(num => print(s" $num "))
      println()
    })
    val coalesce = x.coalesce(2)
    println("\n > changed number of partitions: \n")
    coalesce.foreachPartition(part => {
      println("\n partition:")
      part.foreach(num => print(s" $num "))
      println()
    })
  }

  def repartition(): Unit = {
    println("\n repartition(): \n")
    val x = sc.parallelize(1 to 10, 5)
    x.foreachPartition(part => {
      println(" \npartition: ")
      part.foreach(num => print(s" $num "))
      println()
    })
    val repartition = x.repartition(2)
    println("\n > Changed number of partitions: \n")
    repartition.foreachPartition(part => {
      print("\n partition: ")
      part.foreach(num => print(s" $num "))
      println()
    })
  }

  intersectionAndDistinct()
  repartition()
  coalesce()
}
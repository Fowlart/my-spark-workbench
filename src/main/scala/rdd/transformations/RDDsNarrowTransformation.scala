package rdd.transformations

import org.apache.spark.sql.SparkSession

object RDDsNarrowTransformation extends App {

  val sparkSession = SparkSession.builder()
    .appName("RDDsNarrowTransformation")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  /** =Narrow Transformations:=
   * Output and output stays in the same partition
   *
   * No data movement is needed
   * */

  // flatMapDemo()
  // demoMapPartitions()
  // demoMapPartitionsWithIndex()
  // demoZip()
  // demoZipWithIndex()

  def flatMapDemo() = {
    println("\n FlatMap: \n")
    val collection_flat_map = sc.parallelize(1 to 10).flatMap(x => Seq(x, x))
    println(collection_flat_map.collect().mkString("[", ", ", "]"))
  }


  /** Просто мапить один Partitions до іншого;
   * Функція яка мапить must be of type Iterator => Iterator */
  def demoMapPartitions() = {
    println("\n MapPartitions: \n")
    val rangeWith5Slice = sc.parallelize(1 to 10, 5)
    val mapPartitionsFromRange = rangeWith5Slice.mapPartitions(x => {
      Seq(x.sum).iterator
    }).collect()
    println(mapPartitionsFromRange.mkString("[", ", ", "]"))
  }


  /** Просто мапить один Partitions до іншого.
   *
   * Але з ще додатково для мапінгу результуючого Partitions можна використати індекс initial Partitions. */
  def demoMapPartitionsWithIndex() = {
    println("\n MapPartitionsWithIndex: \n")

    val x = sc.parallelize(1 to 10, 2)
    val z = x.mapPartitionsWithIndex((index, iter) => Seq(s"array: ${iter.mkString("[", ", ", "]")} | index of partition: $index").iterator)
    println(z.collect().mkString("[", ", ", "]"))
  }


  /** Zipping: like union, but elements are allowed to be of the different type */
  def demoZip() = {
    println("\n Zipping: \n")
    val chars = sc.parallelize('A' to 'F')
    val numbers = sc.parallelize(1 to 6)
    val zippedCollection = chars.zip(numbers).collect()
    println(zippedCollection.mkString("[", ", ", "]"))
    println()
  }

  /** Looks like the function only added autoincremented index to the each element of the partition */
  def demoZipWithIndex() = {
    println("\n Zipping with index: \n")
    val chars = sc.parallelize('A' to 'Z')
    val zippedCollection = chars.zipWithIndex().collect()
    println(zippedCollection.mkString("[", ", ", "]"))
    println()
  }

  def unionDemo() = {
    println("\n Union: \n")
    val numbers = sc.parallelize(1 to 6)
    val moreNumbers = sc.parallelize(7 to 10)
    val joinedCollection = numbers.union(moreNumbers).collect()
    println(joinedCollection.mkString("[", ", ", "]"))
  }
}

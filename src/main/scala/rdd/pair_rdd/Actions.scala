package rdd.pair_rdd

import org.apache.spark.sql.SparkSession

object Actions extends App {

  val sparkSession = SparkSession.builder()
    .appName("PairRDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  // countByKeyFunc()
  // collectAsMapFunc()
  // lookupFunc()

  def countByKeyFunc() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val result = x.countByKey()
    println(result)
  }

  /** Will return one value per one key */
  def collectAsMapFunc() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val collectAsMapResult = x.collectAsMap()
    println(collectAsMapResult)
  }

  /** Will return all values per one key */
  def lookupFunc() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val lookupResult = x.lookup('A')
    println(lookupResult)
  }
}

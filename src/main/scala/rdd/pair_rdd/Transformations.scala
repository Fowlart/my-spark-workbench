package rdd.pair_rdd

import org.apache.spark.sql.SparkSession

object Transformations extends App {

  val sparkSession = SparkSession.builder()
    .appName("Pair RDD. Transformations.")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  // mapValues() +
  // flatMapValues() +
  // groupByKey() +
  //reduceByKey()
  // aggregateByKey()
  // sortByKeyFunc()
  // subtractByKeyFunc()
  cogroupFunc()
  // joinFunc()

  /** Will work only with collections of tuples. Will map values. */
  def mapValues() = {
    val x = sc.parallelize(Array(("Olena", 500), ("Artur", 3000), ("Zenovia", 4000)))
    val mapOfValues = x.mapValues(i => i + 500) // each will receive bonus to the salary
    println(mapOfValues.collect().mkString("", " ", ""))
  }

  /** Method is a combination of flatMap and mapValues.
   *
   * First, each value is mapped to the list of values.
   *
   * Second, list of values flattens to separate values but each receives a 'parent key' .
   */
  def flatMapValues() = {
    val rdd = sc.parallelize(Seq(("|", "Roses are red"), ("||", "Violets are blue")))
    val transformed = rdd.flatMapValues(str => str.split(" ")).collect()
    println(transformed.mkString("", "  ", ""))
  }

  def groupByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val groupByKey = x.groupByKey().collect()
    println(groupByKey.mkString("", " ", ""))
  }

  /** Combination groupByKey and reduce. */
  def reduceByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val reduceByKey = x.reduceByKey((acc, item) => acc + item).collect()
    println(reduceByKey.mkString("", " ", ""))
  }

  def aggregateByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4),
      ('B', 5),('A', 6),('B', 7),('B', 8),('B', 9),('A', 10)),2)

    val init_value: String = "EMPTY"
    val f1 = (acc: String, item: Int) => s"$acc|$item"
    val f2 = (acc1: String,acc2: String) => s"$acc1 - $acc2"

    val aggregateByKey = x.aggregateByKey(init_value) (f1,f2)
    println(aggregateByKey.collect().mkString("", " ", ""))
  }

  def sortByKeyFunc() = {
    val x = sc.parallelize(Array(('A', 1), ('C', 2), ('F', 3), ('C', 4), ('G', 5)))
    val z = x.sortByKey().collect()
    println(z.mkString("Array(", ", ", ")"))
  }

  /** Two PairRDD Transformations */
  def subtractByKeyFunc() = {
    val x = sc.parallelize(Array(('C', 1), ('B', 2), ('A', 3), ('A', 4)))
    val y = sc.parallelize(Array(('A', 8), ('B', 7), ('A', 6), ('D', 5)))
    val subtractByKeyResult = x.subtractByKey(y).collect()
    println(subtractByKeyResult.mkString("(", ", ", ")"))
  }

  def cogroupFunc() = {
    val x = sc.parallelize(Array(('C', 1), ('B', 2), ('A', 3),('A', 10),('A', 15)))
    val y = sc.parallelize(Array(('A', 8), ('B', 7), ('A', 6), ('D', 5)))
    val cogroupResult = x.cogroup(y).collect()
    println(cogroupResult.mkString("", " ", ""))
  }

  def joinFunc() = {
    val x = sc.parallelize(Array(('C', 1), ('A', 3), ('A', 4)))
    val y = sc.parallelize(Array(('A', 8), ('B', 7), ('A', 6), ('D', 5)))
    val joinedResult = x.join(y).collect()
    println(joinedResult.mkString("(", ", ", ")"))
  }

}

package rdd

import org.apache.spark.sql.SparkSession

object PairRDDs extends App {

  val sparkSession = SparkSession.builder()
    .appName("PairRDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  /** =Will work only with collections of tuples. Will map values.= */
  def mapValues() = {
    val x = sc.parallelize(Array(("Olena", 500), ("Artur", 3000), ("Zenovia", 4000)))
    val mapOfValues = x.mapValues(i => i + 500) // each will receive bonus to the salary
    println(mapOfValues.collect().mkString("(", ", ", ")"))
  }

  /** =Method is a combination of flatMap and mapValues.=
   * =First, each value is mapped to the list of values.=
   * =Second, list of values flattens to separate values but each receives a 'parent key' .=
   */
  def flatMapValues_2() = {
    val rdd = sc.parallelize(Seq((1, "Roses are red"), (2, "Violets are blue")))
    val transformed = rdd.flatMapValues(str => str.split(" ")).collect()
    println(transformed.mkString("(", ", ", ")"))
  }

  def groupByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val groupByKey = x.groupByKey().collect()
    println(groupByKey.mkString("(", ", ", ")"))
  }

  def reduceByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val reduceByKey = x.reduceByKey((acc, item) => acc + item).collect()
    println(reduceByKey.mkString("(", ", ", ")"))
  }

  // syntax WTF?
  def aggregateByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))

    val aggregateByKey = x.aggregateByKey((0, 0))((acc, item) => (acc._1 + item, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    println(aggregateByKey.collect().mkString("[", ", ", "]"))
  }

  def sortByKeyFunc() = {
    val x = sc.parallelize(Array(('A', 1), ('C', 2), ('F', 3), ('C', 4), ('G', 5)))
    val z = x.sortByKey().collect()
    println(z.mkString("Array(", ", ", ")"))
  }

  /** =Two PairRDD Transformations= */
  def subtractByKeyFunc() = {
    val x = sc.parallelize(Array(('C', 1), ('B', 2), ('A', 3), ('A', 4)))
    val y = sc.parallelize(Array(('A', 8), ('B', 7), ('A', 6), ('D', 5)))
    val subtractByKeyResult = x.subtractByKey(y).collect()
    println(subtractByKeyResult.mkString("(", ", ", ")"))
  }

  def cogroupFunc() = {
    val x = sc.parallelize(Array(('C', 1), ('B', 2), ('A', 3)))
    val y = sc.parallelize(Array(('A', 8), ('B', 7), ('A', 6), ('D', 5)))
    val cogroupResult = x.cogroup(y).collect()
    println(cogroupResult.mkString("Array(", ", ", ")"))
  }

  def joinFunc() = {
    val x = sc.parallelize(Array(('C', 1), ('A', 3), ('A', 4)))
    val y = sc.parallelize(Array(('A', 8), ('B', 7), ('A', 6), ('D', 5)))
    val joinedResult = x.join(y).collect()
    println(joinedResult.mkString("(", ", ", ")"))
  }


  /** =Actions on PairRDD= */
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

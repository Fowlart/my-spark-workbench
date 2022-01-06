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

  def aggregateByKey() = {
    val x = sc.parallelize(Array(('B', 1), ('B', 2), ('A', 3), ('A', 4), ('B', 5)))
    val aggregateByKey = x.aggregateByKey((0, 0))(
      (acc, item) => (acc._1 + item, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    println(aggregateByKey.collect().mkString("[", ", ", "]"))
  }

  aggregateByKey()
}

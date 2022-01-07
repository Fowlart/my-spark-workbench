package rdd.actions

import org.apache.spark.sql.SparkSession

object RDDsActions extends App {

  val sparkSession = SparkSession.builder()
    .appName("RDDsActions")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  /** =Collecting Rows to the Driver:= */

  //takeFunction()
  //topFunction()

  def takeFunction(): Unit = {
    val take = sc.parallelize(Array(1, 2, 1, 3, 2, 5, 8, 7)).take(5)
    println(take.mkString("[", ", ", "]"))
  }

  def topFunction(): Unit = {
    val top = sc.parallelize(Array(1, 2, 1, 3, 2, 5, 8, 7)).top(5)
    println(top.mkString("[", ", ", "]"))
  }

  /**
   * =Data Aggregation:=
   */

  // reduceFunction()
  // foldFunction()
  // aggregateFunction()

  def reduceFunction(): Unit = {
    val sum = sc.parallelize(1 to 10).reduce((acc, item) => acc + item)
    println(s"reduceFunction: $sum")
  }

  def foldFunction() = {
    val x = sc.parallelize(1 to 3)
      .filter(_ > 10) // will receive empty partition
      .fold(0)((acc, item) => acc + item) //0 - initial value, nothing to add to it
    println(s"foldFunction: $x")
  }

  def aggregateFunction() = {

    val rdd = sc.parallelize(1 to 10,2)

    // Initial value to be used for each partition in aggregation, this value would be used to initialize the accumulator.
    // We mostly use 0 for integer and Nil for collections.
    val init_value = 0

    //  This operator is used to accumulate the results of each partition, and stores the running accumulated result to U
    val f1 = (acc: Int, item: Int) => acc+item

    // This operator is used to combine the results of all partitions U
    val f2 = (acc1: Int, acc2: Int) => acc1+acc2

    val x = rdd.aggregate(init_value)(f1, f2)

    println("aggregateFunction: " + x)
  }

  /** =Data Saving:= */

  // saveSEQUANCE()
  //  saveTXT()

  def saveTXT() = {
    sc.parallelize(1 to 100, 2)
      .saveAsTextFile("src/main/resources/nums_txt")
  }

  def saveSEQUANCE() = {
    sc.parallelize(1 to 100, 2)
      .map(num => (s"=$num=", num))
      .saveAsSequenceFile("src/main/resources/nums_seq")
  }
}

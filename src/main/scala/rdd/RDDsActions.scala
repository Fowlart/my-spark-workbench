package rdd

import org.apache.spark.sql.SparkSession

object RDDsActions extends App {

  val sparkSession = SparkSession.builder()
    .appName("RDDsActions")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  /** =Collecting Rows to the Driver:= */
  def takeFunction(): Unit = {
    val take = sc.parallelize(Array(1, 2, 1, 3, 2, 5, 8, 7)).take(3)
    println(take.mkString("[", ", ", "]"))
  }

  def topFunction(): Unit = {
    val top = sc.parallelize(Array(1, 2, 1, 3, 2, 5, 8, 7)).top(3)
    println(top.mkString("[", ", ", "]"))
  }

  // takeFunction()
  // topFunction()

  /**
   * =Data Aggregation:=
   */
  def reduceFunction(): Unit = {
    val sum = sc.parallelize(1 to 10).reduce((acc, item) => acc + item)
    println(s"reduceFunction: $sum")
  }

  def foldFunction() = {
    val x = sc.parallelize(1 to 3)
      .filter(_ > 10) // will receive empty partition
      .fold(0)((acc, item) => acc + item) //0 - initial value, nothing to add to it

    println(s"foldFunction: $x")

    val y = sc.parallelize(1 to 3)
      .filter(_ > 10) // will receive empty partition
      .reduce((acc, item) => acc + item) // exception

    println(y)

  }

  def aggregateFunction() = {
    val listRdd = sc.parallelize(List(1, 2, 3, 4, 5, 3, 2), 2)

    // This operator is used to accumulate the results of each partition, and stores the running accumulated result to U
    def param0 = (accu: Int, v: Int) => accu + v

    // This operator is used to combine the results of all partitions U
    def param1 = (accu1: Int, accu2: Int) => accu2 + accu1
    // accu1 - has no influence un the case, we have only 1 partition

    val result = listRdd.aggregate(0)(param0, param1)
    println("output 1: " + result)
  }

  try {
    // foldFunction()
  }
  catch {
    case e: Exception => println(e)
  }
  // reduceFunction()
  // aggregateFunction()

  /** =Data Saving:= */
  def saveTXT()={
    sc.parallelize(1 to 100, 2)
      .saveAsTextFile("src/main/resources/nums_txt")
  }

  saveTXT()

  def saveSEQUANCE() = {
    sc.parallelize(1 to 100, 2)
      .map(num=>(s"=$num=",num))
      .saveAsSequenceFile("src/main/resources/nums_seq")
  }

  saveSEQUANCE()
}

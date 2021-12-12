package spark_shell

import playground.Playground.spark

object SparkShell extends App {

  val sc = spark.sparkContext
  val rdd1 = sc.parallelize(1 to 1000000000)
  val rdd1_count = rdd1.count
  val rdd1_partition_num = rdd1.getNumPartitions
  val rdd2 = rdd1.map(_ * 2)
  val rdd2_count = rdd2.count

  rdd1.repartition(21).count
  // count will create operation per each partition(21 + ...)  - we will receive

  import spark.implicits._

  rdd1.toDF().show()


  ds1.show
  ds1.explain
  ds1.explain(true)

  val ds2 = spark.range(0, 10000000, 15)

  val ds1 = spark.range(0, 100000, 5)
  val ds3 = ds1.repartition(7)
  val ds5 = ds3.selectExpr("id * 5 as id")

  ds3.explain
  ds5.explain

  val ds6 = ds5.join(ds3, "id")
  ds6.explain

  val ds7 = ds6.selectExpr("sum(id)")
  ds7.explain
  ds7.show

  val df1 = sc.parallelize(1 to 10000).toDF.repartition(7)
  val df2 = sc.parallelize(1 to 10000).toDF.repartition(9)
  val df3 = df1.selectExpr("value * 5 as value")
  val complexJoined = df3.join(df2, "value")
  complexJoined.explain
  complexJoined.show
}
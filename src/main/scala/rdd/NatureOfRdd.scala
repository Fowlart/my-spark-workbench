package rdd

import org.apache.spark.sql.SparkSession

object NatureOfRdd extends App {

  val sparkSession = SparkSession.builder()
    .appName("NatureOfRdd")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val rdd = sc.parallelize(List(("Artur",3000),("Olena",5000),("Vita",2000),("Zenuk",4500)),2)

  rdd.saveAsTextFile("src/main/resources/tuples_txt")
}

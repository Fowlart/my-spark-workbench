// exclude the file from compilation if You are using Idea
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Spark SQL")
  .config("spark.master", "local")
  .getOrCreate()

// Databricks notebook source
val sc = spark.sparkContext

val dataSourcePath = s"/opt/spark-data/movies.json"

// COMMAND ----------

sc.setJobDescription("Step B: Read and cache the initial DataFrame")

val initialDF = spark.read.option("inferSchema", "true").json(dataSourcePath).cache()

initialDF.count()

// COMMAND ----------

sc.setJobDescription("Step C: A bunch of random transformations")

val someDF = initialDF.withColumn("first", upper(col("Title").substr(0, 1))).where(col("first").isin("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O")).groupBy(col("first")).agg(sum(col("US_Gross")).as("total")).orderBy(col("first")).filter(col("total") > 400)

val total = someDF.count().toInt

// COMMAND ----------

sc.setJobDescription("Step D: Take N records")

someDF.take(total)

// COMMAND ----------

sc.setJobDescription("Step E: Create a really big DataFrame")

var bigDF = initialDF

for (i <- 0 to 6) bigDF = bigDF.union(bigDF).repartition(sc.defaultParallelism)

bigDF.count()

/** The Spark API uses the term core to mean a thread available for parallel execution.
 * To avoid confusion with the number of cores in the underlying CPU(s),
 * these are also sometimes referred to as slots. If you created your cluster,
 * you likely know how many cores you have. However, to check programmatically,
 * you can use SparkContext.defaultParallelism. */
sc.defaultParallelism

/** To allow every executor to perform work in parallel,
 * Spark breaks up the data into chunks called partitions.
 * A partition is a collection of rows that sit on one physical machine on your cluster.
 * A DataFrame's partitions represent how the data is physically distributed across the cluster of machines during execution. */
bigDF.rdd.getNumPartitions

sc.setJobDescription("Step F: coalesce")
val newDecreasedRdd = bigDF.rdd.coalesce(2)
newDecreasedRdd.count()

sc.setJobDescription("Step I: repartition")
bigDF.rdd.repartition(8).count()

/** Configure default shuffle partitions */
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.shuffle.partitions", "4")

/** In Spark 3, AQE is now able to dynamically coalesce shuffle partitions at runtime */
spark.conf.get("spark.sql.adaptive.enabled")
spark.conf.set("spark.sql.adaptive.enabled", "true")


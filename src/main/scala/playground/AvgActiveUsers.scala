package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate

object AvgActiveUsers extends App {

  lazy val avgUq = orderedUserQuantitiesByEntityId
    .collectAsList
    .stream
    .mapToInt(row => row.getInt(0))
    .average

  val spark = SparkSession.builder().appName("Joins").config("spark.master", "local").getOrCreate()

  val entityId = 1107

  val userQuantities = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://35.231.206.178/lb-rest-api-gcp-develop")
    .option("user", "lb-rest-api-gcp-develop")
    .option("password", "ptqQ6ZL3Kqg44Oor")
    .option("dbtable", "fs_active_users_quantities")
    .option("inferSchema", "true")
    .load()

  val columnForSelection = col("active_application_users_quantities")
  val dateColumn = to_date(col("date")).as("date")

  val now = LocalDate.now()

  val orderedUserQuantitiesByEntityId = userQuantities.select(columnForSelection, dateColumn)
    .filter(col("entity_id") === entityId)
    .filter(dateColumn.between(now.minusDays(3), now))
    .orderBy("id");

  // orderedUserQuantitiesByEntityId.show()

  val deeperAnalysis = orderedUserQuantitiesByEntityId.select(
    avg(columnForSelection).as("avg"),
    min(columnForSelection).as("min"),
    max(columnForSelection).as("max"),
    sum(columnForSelection).as("sum"),
    stddev(columnForSelection).as("stddev"),
    countDistinct(columnForSelection).as("countDistinct"),
    count(columnForSelection).as("count")
  )

  deeperAnalysis.show()
  val grouped = orderedUserQuantitiesByEntityId.groupBy(columnForSelection).count()
  //  grouped.show()
}

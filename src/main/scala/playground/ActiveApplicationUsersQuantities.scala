package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, to_date}

import java.time.LocalDate

object ActiveApplicationUsersQuantities extends App {

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val userQuantities = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://35.231.206.178/stage-foodsense")
    .option("user", "postgres")
    .option("password", "wqpwwIb8g7nz6al7a791roJlL")
    .option("dbtable", "fs_active_users_quantities")
    .option("inferSchema", "true")
    .load()

  val applicationUsers = col("active_application_users_quantities")
  val dateColumn = to_date(col("date")).as("date")
  val entityId = col("entity_id")
  val now = LocalDate.now()
  val daysFilter = dateColumn.between(now.minusDays(30), now)

  userQuantities
    .groupBy(entityId)
    .avg("active_corporate_users_quantities")
    .orderBy(desc("avg(active_corporate_users_quantities)"))
    .show(60)


}

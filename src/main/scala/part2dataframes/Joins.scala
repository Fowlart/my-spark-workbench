package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  guitarPlayers.join(bands, joinCondition, "inner")
  guitarPlayers.join(bands, joinCondition, "left_outer")
  guitarPlayers.join(bands, joinCondition, "right_outer")
  guitarPlayers.join(bands, joinCondition, "full_outer")

  //semi-joins - same as inner, but only with left-hand table data
  guitarPlayers.join(bands, joinCondition, "left_semi")

  //anti-joins - anti-joins - we will have rows from the left-hand side table, that does not satisfy the condition
  guitarPlayers.join(bands, joinCondition, "left_anti")

  // will crash because of multiply 'id' column
  //  guitarPlayers.join(bands, joinCondition, "inner").select("id")
  //option to resolve:
  //1 - rename column(using keyword in SQL)
  guitarPlayers.join(bands.withColumnRenamed("id", "band"), "band").select("id")
  //2 - drop the duplicate column[NOT WORKING]
  guitarPlayers.join(bands, joinCondition, "inner").drop(bands.col("id"))

  //using complex types
  guitarPlayers.join(guitars.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars,guitarId)"))

  /**
    * Exercises
    *
    *   1 - show all employees and their max salaries;
    *   2 - show all employees who were not been managers;
    *   3 -find the jobs titles of the best paid employees
    */

  val employees = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  val salaries = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .load()

  val deptManager = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.dept_manager")
    .load()

  // 1 - show all employees and their max salaries
  employees.join(salaries,"emp_no").groupBy(col("emp_no")).agg(max(col("salary")))
    .orderBy(col("emp_no"))

  // 2 - show all employees who were not been managers
  employees.filter(col("emp_no")=!=deptManager.select(col("emp_no"))).select(col("emp_no"))



}

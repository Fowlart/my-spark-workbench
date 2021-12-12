package part4spark_sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .getOrCreate()

  val cars = spark
    .read
    .json("src/main/resources/data/cars.json")

  cars.createOrReplaceTempView("cars") // will create table on operation memory

  // using plain SQL
  spark.sql(
    """select Origin as origin, avg(Acceleration) as avg_acceleration
      |from cars
      |group by Origin""".stripMargin)

  import spark.implicits._

  case class TableNames(tablename: String) {
    override def toString: String = tablename
  }

  // read all tables from remote database
  val tableNames = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://35.231.206.178/stage-foodsense")
    .option("user", "postgres")
    .option("password", "wqpwwIb8g7nz6al7a791roJlL")
    .option("query",
      """SELECT tablename
  FROM pg_catalog.pg_tables
  WHERE schemaname != 'pg_catalog' AND
  schemaname != 'information_schema'""".stripMargin)
    .load().as[TableNames].collectAsList()

  // convert remote tables into local parquet files
  def readAndSaveTable(tableNames: java.util.List[TableNames]): Unit = {
    tableNames.forEach(name => {
      println(s"processing table with name ${name.tablename}")
      spark
        .read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://35.231.206.178/stage-foodsense")
        .option("user", "postgres")
        .option("password", "wqpwwIb8g7nz6al7a791roJlL")
        .option("dbtable", s"${name.tablename}")
        .option("inferSchema", "true")
        .load()
        // move all table in OM
        //.createOrReplaceTempView(s"${name.tablename}")
        .write
        .mode(SaveMode.Overwrite)
        // move all tables on the disk
        .saveAsTable(s"${name.tablename}")
    })
  }

  // spark.sql("select * from fuser").show()
  // readAndSaveTable(tableNames)
  // userQuantities.write.mode(SaveMode.Overwrite).saveAsTable("user_quantities")
  // userQuantities.createOrReplaceTempView("user_quantities") // will create table on operation memory

  // read from parquet
  val fuser = spark
    .read
    .load("spark-warehouse/fuser")

  // retrieve data from parquet
  case class UserWithEmail(user_id: String, email: String) {
    override def toString: String = {
      s"[$user_id - $email]"
    }
  }

  fuser.as[UserWithEmail]

  /**
    * Task-1: transfer table to another DB
    * */
  fuser.createOrReplaceTempView("fuser")

  val fusersWithSomeDates = spark
    .sql("select * from fuser where pos_sync_date is not null")

  println(fusersWithSomeDates.schema)

  /**
    * Task-2: filtering by date
    **/
  fusersWithSomeDates.filter("pos_sync_date > timestamp '2021-05-01 12:00'").show()


  /**
    * Task-3: use joins
    **/
  val fuser_entity = spark
    .read
    .load("spark-warehouse/fuser_entity")

  fuser_entity.createOrReplaceTempView("fuser_entity")

  spark.sql("select f.user_id, fe.entity_id from fuser_entity fe join fuser f on (f.user_id = fe.user_id ) where fe.entity_id = 1").show()

}

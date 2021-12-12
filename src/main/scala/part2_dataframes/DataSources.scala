package part2_dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val sparkSession = SparkSession.builder()
    .appName("DataSources 2")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(StructField("Acceleration", DoubleType, nullable = true, Metadata.empty),
    StructField("Year", DateType)))

  /** Reading DF
    * + format
    * + schema or inferSchema = true
    * + zero or more options
    */
  val carsDF = sparkSession.read
    .schema(carsSchema)
    //   .option("inferSchema", "true")
    .option("mode", "permissive") //dropMalformed, permissive(default), failFast
    .option("dateFormat", "dd-mm-yyyy")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2,gzip... > will unzip automatically
    .json("src/main/resources/data/cars_test.json")


  /** Writing DF
    * Will be called from the DF object. Obligated params:
    * 1 - format
    * 2 - save mode = overwrite, append, ignore, errorIfExists
    * 3 - path
    * 4 - zero or more options */
  carsDF
    .write
    .mode(SaveMode.Overwrite)
    .json("src/main/resources/data/cars_test_1.json")

  /** CSVs
    *
    */
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)))

  val stockDf = sparkSession
    .read
    .schema(stockSchema)
    .option("header", "true") // will try to parse header as a data if not specified
    .option("dateFormat", "MMM d yyyy")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  /** Parquet:
    * Apache Parquet is designed for efficient as well as performant flat columnar storage format
    * of data compared to row based files like CSV or TSV files.
    */
  stockDf.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks.parquet")

  /** Text file */
  sparkSession
    .read
    .text("src/main/resources/data/sample_text.txt")

  /** Reading from remote DB */
  val employeesDf = sparkSession.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDf.show()
}

package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val spark = SparkSession.builder().appName("Data Sources").config("spark.master", "local").getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /** Reading any DF needs:
    *  - format
    *  - schema or inferSchema = true
    *  - options
    * */

  val carsDfWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .load("src/main/resources/data/cars.json")

  carsDfWithSchema.show()

  //Writing DF

  /**
    * Writing any DF needs:
    *  - format
    *  - save mode = overwrite, append, ignore, errorIfExist
    *  - path
    *  - options
    * */

  carsDfWithSchema.write.format("json").mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_1.json")

  /** Options for reading JSONs */

  val carsEnhancedSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val carsEnhanced = spark.read
    .schema(carsEnhancedSchema)
    .option("dateFormat","dd-MM-YYYY") // couple with schema; if spark failed parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2,gzip... > will unzip automatically
    .json("src/main/resources/data/cars.json")

  carsEnhanced.show()

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stocksDf = spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsEnhanced
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars_1.parquet")

  // Text files
  val text = spark.read.text("src/main/resources/data/sample_text.txt")
  text.show()

  // READING FROM A REMOTE DB
  val employeesItemsDs = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesItemsDs
    .show()

  /** Tasks:
    * 1 - read the movies dataframe;
    * 2 - write it as tab separated csv;
    * 3 - write it as parquet;
    * 4 -write at the DB as public.movies. */

  // 1
  val moviesJsonSchema = StructType(Array(
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Distributor", StringType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", IntegerType),
    StructField("MPAA_Rating", StringType),
    StructField("Major_Genre", StringType),
    StructField("Production_Budget", LongType),
    StructField("Rotten_Tomatoes_Rating", IntegerType),
    StructField("Running_Time_min", IntegerType),
    StructField("Source", StringType),
    StructField("Title", StringType),
    StructField("US_DVD_Sales", LongType),
    StructField("US_Gross", LongType),
    StructField("Worldwide_Gross", LongType)
  ))

  val movies = spark
    .read
    .option("dateFormat", "dd-MMM-YY")
    .schema(moviesJsonSchema)
    .json("src/main/resources/data/movies.json")

  movies.show(5)

  // 2
  movies
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "  ") // tab
    .option("nullValue", "null")
    .csv("src/main/resources/data/movies.csv")

  // 3
  movies
    .write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()


}

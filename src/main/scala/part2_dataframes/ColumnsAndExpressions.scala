package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types._

object ColumnsAndExpressions extends App {

  val sparkSession = SparkSession.builder()
    .appName("Columns And Expressions")
    .config("spark.master", "local")
    .getOrCreate()

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

  // column is plain jvm object that do not has any data
  val priceColumn = stockDf.col("price")

  // selecting is called 'projection'
  stockDf.select(priceColumn)

  // required to use #import org.apache.spark.sql.functions.col
  stockDf.select(col("symbol"), col("date"), column("price"))
  println(priceColumn.equals(col("price"))) // false, hah =)
  println(col("price").equals(col("price"))) // true
  println(col("price").equals(column("price"))) // true

  // using an expr
  stockDf.select(expr("COUNT (price)"))
  val weightInUah = (col("price") * 26.3).as("price in UAH")
  println(s"weightInUah.getClass: ${weightInUah.getClass}") // Column class
  stockDf.select(weightInUah)

  val weightInUahExpr = expr("price * 26.3").as("price in UAH")
  stockDf.select(weightInUahExpr)
  println(s"weightInUahExpr.getClass: ${weightInUahExpr.getClass}") // Column class

  stockDf.selectExpr("price * 26.3", "symbol")
    .withColumnRenamed("(price * 26.3)", "price in UAH")

  // adding new column
  val stockDfWithNewColumn = stockDf.withColumn("price in UAH", expr("price * 26.3"))

  // dropping new column
  val optimizedStockDfWithNewColumn = stockDfWithNewColumn.drop("price")

  // filtering
  val predicate = col("price in UAH") > 1000
  optimizedStockDfWithNewColumn.filter(predicate)
  optimizedStockDfWithNewColumn.where(predicate)

  //Union
  val moreStocks = sparkSession
    .read
    .schema(stockSchema)
    .option("header", "true") // will try to parse header as a data if not specified
    .option("dateFormat", "MMM d yyyy")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/more_stocks.csv")

  val unionOfStocks = stockDf.union(moreStocks)
  unionOfStocks.show()


}

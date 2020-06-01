package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {
  // entry point for creating, reading or writing the data frames
  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()
  // reading a DF
  // data frame reader with inferSchema and path
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true") // all the columns of this data frame be from the json structure
    .load("src/main/resources/data/cars.json")
  // => data frame
  // IMPORTANT: in production inferSchema is not recommended - year is a string in json and it's not ISO format
  // use your own schema

  // showing a DF
  firstDF.show() // display table
  firstDF.printSchema() // returns the description of the columns!

  // get rows
  firstDF.take(10).foreach(println) // takes out the first 10 rows
  // array of rows!

  // A Data frame is:
  // a schema that contains the attributes of the data
  // a distributed collections of rows that are conforming with that schema

  // spark types
  val longType = LongType // case objects that describe

  // create schema by yourself
  // struct type that contains struct fields in sequence (array)
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // obtain a schema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema) // similar with carsSchema

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  // factory method
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  // seq of tuples of rows
  // Create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )
  val manualCarsDf = spark.createDataFrame(cars) // schema auto-inferred
  // no column names!

  // ROWS - unstructured data

  // note: DFs have schemas, rows do not

  //c create DFs with implicits

  import spark.implicits._

  val manualCarsWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acc", "Year", "Country")

  manualCarsDf.printSchema()
  manualCarsWithImplicits.printSchema()

  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *  - make
   *  - model
   *  - ram
   *  - screen dimension
   *  - camera megapixels
   * -> show and printSchema
   * 2) Read another file from the data/ folder e.g. movies.jpg
   * - print its schema
   * - count the number of rows, call count()
   *///
  // 1
  val smartphones = Seq(
    ("Apple", "Iphone X", "3GB", "1080x1020", 20),
    ("Samsung", "R2", "2GB", "800x600", 10),
    ("Nokia", "X10", "1GB", "600x200", 5)
  )
  val smartphonesDF = smartphones.toDF("Make", "Model", "RAM", "Screen Dimension", "Camera Megapixels")
  smartphonesDF.show()
  smartphonesDF.printSchema()
  // 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(moviesDF.count())
}

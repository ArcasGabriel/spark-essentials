package part2dataframes

object DataSources extends App {

  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.types._


  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if spark fails parsing, it will put null!!
    .option("allowSingleQuotes", "true") // double quotes = single quotes
    .option("compression", "uncompressed") // bzip2, gzip, lz4 (7zip), snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM DD YYYY") // schema important!
    .option("header", "true") // may have a header! - ignores the first row, check column names.
    .option("sep", ",") // in practice may have tab or other separators
    .option("nullValue", "") // no notion of null! parse this value as null.
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  // an open source compressed binary data storage format optimized for fast reading of columns
  // works so well with spark - default format for data frames
  // you do not need so much options as csv!
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet") // automatically find the format parquet
  // If you compare it with json file, is it quite smaller than the json file
  // so the compress is very important when you dealing with huge files


  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()
  employeesDF.show()

  /**
   * Read the movies.json as DF, then write it:
   * - tab-separated values file (csv with separator)
   * - snappy parquet
   * - table in the postgres DB **/

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")
  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "movies")
    .save("src/main/resources/data/movies.jdbc")
}

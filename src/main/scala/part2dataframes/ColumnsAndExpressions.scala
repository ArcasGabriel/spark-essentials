package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  // res: column object
  val firstColumn = carsDF.col("Name")
  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn) // new data frame!

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, return a column object
    expr("Origin") // expression
  )
  // select with plain column names
  carsDF.select("Name", "Year")

  // selecting - extract columns from all nodes (partitions)
  // after select, a new data frame with all data from nodes
  // and will be reflected to other nodes
  // narrow transformation! every node data correspond to the output node

  // EXPRESSIONS
  val simplestExpressions = carsDF.col("Weight_in_lbs") // sub type of expression
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  // column object that contains transformation
  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")

  )
  carsWithWeightDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  // adding a column
  val carsWithKG3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("'Weight in pounds'") // escape spaces!
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA") // not equal with USA
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  // ==  -  ===
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have same schema

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show() // display only origins (3 lines)

  /**
   *
   * 1. Read the movies.json DF and select 2 columns of your choice (Title, Release_Date)
   * 2. Create another column summing up the total profit of the movie: US_Gross + WorldWide_Gross + US_DVD
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible!
   */
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  val moviesTitleAndReleaseDate = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date")
  )
  val moviesProfitDF = moviesDF
    .withColumn("Total Profit", col("US_Gross") + moviesDF.col("Worldwide_Gross"))

  val comedyMoviesHighRatedDF = moviesDF.select(col("Title"), col("IMDB_Rating"))
    .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comedyMoviesHighRatedDF.show()
}

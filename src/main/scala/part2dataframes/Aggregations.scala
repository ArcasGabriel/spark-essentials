package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  genresCountDF.show()

  moviesDF.selectExpr("count(Major_Genre)") // equivalent
  // counting all
  moviesDF.select(count("*")) // count all the rows, and will include NULLS
  // counting distinct
  moviesDF.select(countDistinct("Major_Genre")).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) // big data frames when we want quick analysis

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_GROSS"))
  moviesDF.selectExpr("sum(US_GROSS)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  // mean - average
  // stddev - how close or how far away is rotten tomatoes rating against the mean
  // higher is far away
  // res: 28 - values are spread out: has both poor movies and good movies
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  // groupBy -  RelationalGroupedDataset
  // gorup by includes null!!
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // select count(*) from MoviesDF group by Major_Genre
  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggreationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_rating"))
  // agg is very powerful! 2 aggregations or more
  aggreationsByGenreDF.show()

  /**
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US_Gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US Gross revenue PER Director (sort)
   */
  // 1
  val moviesProfitsDF = moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profit"))
    .agg(
      sum("Total_Profit")
    )
  moviesProfitsDF.show()
  // 2
  val moviesDirectorsDF = moviesDF.select(countDistinct("Director").as("Directors"))
  moviesDirectorsDF.show()
  // 3
  val moviesUsRevenue = moviesDF.select(
    mean("US_Gross").as("AVG_US_GROSS"),
    stddev("US_GROSS").as("STTDEV_US_GROSS")
  )
  moviesUsRevenue.show()
  val moviesDirectorRating = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("AVG_IMDB_RATING"),
      sum("US_Gross").as("SUM_US_GROSS")
    )
    .orderBy(col("AVG_IMDB_RATING").desc_nulls_last)
  moviesDirectorRating.show()
}

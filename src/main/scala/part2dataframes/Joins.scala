package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // bands - rock and roll bands
  // guitar players
  // guitars model

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins - WIDE TRANSFORMATIONS
  // inner - all the columns from the guitarists and from the band
  //  - all the rows that satisfy the condition, the others will be discarded

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristBandsDF.show()

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join  = everything in the inner join + all the rows in BOTH tables (left + right), with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // things to bear in mind
  // guitaristBandsDF.select("id","band").show() // this crashes!

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  // option 2- drop the dupe column
  // hey spark, drop me the column with unique id from band df
  guitaristBandsDF.drop(bandsDF.col("id"))
  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars,guitarId)"))

  /**
   * Exercises
   *
   * - show all employees and their max salary (has an emp_no)
   * - show all employees who were never managers (table call department manager, emp_no)
   *    - all the employees that are not in this table
   * - find the job titles of the best paid 10 employees in the company (titles table (max in to_date column))
   */
  // Global vars for postgres
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()


  // DB reading from spark
  val employeesDF = readTable("employees")

  val salariesDF = readTable("salaries")

  val deptManagerDF = readTable("dept_manager")

  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDf = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeeSalariesDF = employeesDF.join(maxSalariesPerEmpNoDf, "emp_no")


  // 2
  val employeesManagersCond = employeesDF.col("emp_no") === deptManagerDF.col("emp_no")
  employeesDF
    .join(deptManagerDF, employeesManagersCond, "left_anti")
    .show()

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no","title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeeSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()


}

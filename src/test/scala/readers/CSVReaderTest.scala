package readers

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CSVReaderTest extends AnyFunSuite with Matchers {

  // Create a SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("CSVReaderTest")
    .master("local[*]")
    .getOrCreate()

  // Initialize the CSVReader with the SparkSession
  val csvReader = new CSVReader(spark)

  // Define the path to the test CSV file
  val testCSVPath = "src/test/resources/test.csv"

  test("CSVReader should read CSV file correctly with default options") {
    // Read the CSV file into a DataFrame
    val df = csvReader.readCSV(testCSVPath)

    // Assert the DataFrame is not empty and has the correct schema
    assert(!df.isEmpty, "The DataFrame should not be empty")
    df.columns should contain allOf ("id", "name")
    assert(df.count() == 3, "The DataFrame should contain exactly 3 rows")
  }

  test("CSVReader should apply custom options correctly") {
    // Provide custom options
    val customOptions = Map("header" -> "true", "delimiter" -> ",")

    // Read the CSV file with the custom options
    val df = csvReader.readCSV(testCSVPath, customOptions)
    df.show(false)

    // Assert the DataFrame contains the expected data
    assert(!df.isEmpty, "The DataFrame should not be empty")
    df.select("name").collect().map(_.getString(0)) should contain allOf ("Alice", "Bob", "Charlie")
  }
}
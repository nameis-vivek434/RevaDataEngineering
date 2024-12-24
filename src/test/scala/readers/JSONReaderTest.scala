package readers

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JSONReaderTest extends AnyFunSuite with Matchers {

  // Create a SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("JSONReaderTest")
    .master("local[*]")
    .getOrCreate()

  // Initialize the JSONReader with the SparkSession
  val jsonReader = new JSONReader(spark)

  // Define the path to the test JSON file
  val testJSONPath = "src/test/resources/test.json"

  test("JSONReader should read JSON file correctly with default options") {
    // Read the JSON file into a DataFrame
    val df = jsonReader.readJSON(testJSONPath)

    // Assert the DataFrame is not empty and has the correct schema
    assert(!df.isEmpty, "The DataFrame should not be empty")
    df.columns should contain allOf("id", "name")
    assert(df.count() == 3, "The DataFrame should contain exactly 3 rows")
  }

  test("JSONReader should handle multiline JSON files") {
    // Provide custom options for multiline JSON
    val multilineOptions = Map("multiline" -> "true")

    // Read the JSON file with the multiline option
    val df = jsonReader.readJSON(testJSONPath, multilineOptions)

    // Assert the DataFrame contains the expected data
    assert(!df.isEmpty, "The DataFrame should not be empty")
    df.select("id").collect().map(_.getInt(0)) should contain allOf(1, 2, 3)
  }
}

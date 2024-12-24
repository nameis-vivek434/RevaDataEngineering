package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVReader(spark: SparkSession) {

  /**
   * Reads a CSV file from the specified path and returns it as a DataFrame.
   *
   * @param path    The path to the CSV file.
   * @param options Map of options like delimiter, header, etc. (optional).
   * @return A DataFrame containing the CSV data.
   */
  def readCSV(path: String, options: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")): DataFrame = {
    spark.read
      .options(options) // Set options (e.g., header=true, inferSchema=true)
      .csv(path) // Load the CSV file
  }
}
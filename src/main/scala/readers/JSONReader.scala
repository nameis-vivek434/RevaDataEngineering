package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

class JSONReader(spark: SparkSession) {

  /**
   * Reads a JSON file from the specified path and returns it as a DataFrame.
   *
   * @param path    The path to the JSON file.
   * @param options Map of options like multiline, etc. (optional).
   * @return A DataFrame containing the JSON data.
   */
  def readJSON(path: String, options: Map[String, String] = Map("multiline" -> "false")): DataFrame = {
    spark.read
      .options(options) // Set optional configurations like multiline=true
      .json(path) // Load the JSON file
  }
}

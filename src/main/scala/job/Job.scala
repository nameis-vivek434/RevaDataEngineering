package job

import org.apache.spark.sql.{Dataset, SparkSession}

class Job(spark: SparkSession) {

  /**
   * Performs a simple operation on input data and writes output.
   *
   * @param inputPath  Path to the input data.
   * @param outputPath Path where the output data will be written.
   */
  def run(inputPath: String, outputPath: String): Unit = {
    val inputDF = readCSV(inputPath)
    val processedDF = process(inputDF)
    writeCSV(processedDF, outputPath)
  }

  private def readCSV(path: String) = {
    spark.read
      .option("header", "true")
      .csv(path)
  }

  private def process(dataset: Dataset[_]) = {
    dataset.filter(dataset("value") =!= "null") // Example transformation
  }

  private def writeCSV(dataset: Dataset[_], path: String): Unit = {
    dataset.write
      .option("header", "true")
      .csv(path)
  }
}
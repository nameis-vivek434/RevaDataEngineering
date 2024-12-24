package utils

import org.apache.spark.sql.SparkSession

object SparkSessionManager {
  // Initialize the SparkSession
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]") // Change this based on the cluster mode (e.g., "yarn")
      .config("spark.some.config.option", "some-value") // Add additional configs if needed
      .getOrCreate()
  }
}

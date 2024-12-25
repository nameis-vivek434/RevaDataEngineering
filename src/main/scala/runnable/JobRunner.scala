package runnable

import job.Job
import org.apache.spark.sql.SparkSession

object JobRunner {
  /**
   * Main entry point for running the job.
   * This object is designed to be called from the command line or spark-submit.
   *
   * @param args Array of arguments for input/output paths.
   *             args(0): Input file path
   *             args(1): Output file path
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new IllegalArgumentException("Please provide both input and output file paths!")
    }

    val inputPath = args(0)
    val outputPath = args(1)

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("JobRunner")
      .getOrCreate()

    try {
      val job = new Job(spark)
      job.run(inputPath, outputPath) // Run the job
    } finally {
      spark.stop()
    }
  }
}
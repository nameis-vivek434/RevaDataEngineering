package job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Job {

  def processNumbers(numbers: RDD[Int]): RDD[Int] = {
    if (numbers.isEmpty()) {
      throw new IllegalArgumentException("Input RDD cannot be empty")
    }

    // Double each number
    numbers.map(_ * 2)
  }

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("NumberDoublingJob")
      .master("local[*]")  // Setting for local testing
      .getOrCreate()

    // Example input numbers
    val input = Seq(1, 2, 3, 4, 5)
    val inputRDD = spark.sparkContext.parallelize(input)

    // Call processNumbers
    val resultRDD = processNumbers(inputRDD)

    // Collect and print result
    resultRDD.collect().foreach(println)

    // Stop SparkSession
    spark.stop()
  }
}
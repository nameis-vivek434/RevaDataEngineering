// File: NumberDoublingJobTest.scala

import job.Job
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.rdd.RDD

class NumberDoublingJobTest extends AnyFunSuite {

  // Create a shared SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("NumberDoublingJobTest")
    .master("local[*]")  // Local Spark master
    .getOrCreate()

  import spark.implicits._

  test("processNumbers doubles the numbers correctly") {
    val input = Seq(1, 2, 3, 4, 5)
    val inputRDD: RDD[Int] = spark.sparkContext.parallelize(input)

    val resultRDD: RDD[Int] = Job.processNumbers(inputRDD)
    val result: Array[Int] = resultRDD.collect()

    assert(result === Array(2, 4, 6, 8, 10))
  }

  test("processNumbers throws exception for empty RDD") {
    val emptyRDD: RDD[Int] = spark.sparkContext.emptyRDD[Int]

    val exception = intercept[IllegalArgumentException] {
      Job.processNumbers(emptyRDD).collect()
    }

    assert(exception.getMessage === "Input RDD cannot be empty")
  }
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Date
class FlightDataAnalysisTest extends AnyFunSuite with Matchers {
  val spark: SparkSession = SparkSession.builder()
    .appName("Flight Data Analysis Tests")
    .master("local[*]")
    .getOrCreate()

  import FlightDataAnalysis._
  import spark.implicits._

   /**
   * Tests for answerQuestion1
   */
  test("answerQuestion1 should count flights per month correctly") {
    // Sample flight data
    val flights = Seq(
      Flight("p1", "f1", "US", "UK", Date.valueOf("2023-01-15")),
      Flight("p2", "f2", "CA", "FR", Date.valueOf("2023-02-20")),
      Flight("p3", "f3", "UK", "DE", Date.valueOf("2023-02-25")),
      Flight("p4", "f4", "FR", "US", Date.valueOf("2023-03-10")),
      Flight("p5", "f5", "DE", "CA", Date.valueOf("2023-03-15"))
    ).toDS()

    // Expected result
    val expectedResult = Seq(
      (1, 1L),
      (2, 2L),
      (3, 2L)
    ).toDF("Month", "Number of Flights")

    // Running the function under test
    val result = answerQuestion1(flights)(spark)

    // Assertion
    assertDataFrameEqualsQ1(expectedResult, result)
  }

  // Helper function to compare DataFrames
  def assertDataFrameEqualsQ1(expected: DataFrame, actual: DataFrame): Unit = {
    val e = expected.orderBy(expected.columns.map(col): _*).collect()
    val a = actual.orderBy(actual.columns.map(col): _*).collect()
    assert(e.sameElements(a), s"\nExpected: ${e.mkString("Array(", ", ", ")")}\nActual:   ${a.mkString("Array(", ", ", ")")}")
  }
  /**
   * Tests for answerQuestion2
   */
  test("answerQuestion2 should aggregate and join flights and passengers correctly") {
    // Sample flight data
    val flights: Dataset[Flight] = Seq(
      Flight("P1", "F1", "USA", "UK", Date.valueOf("2023-01-01")),
      Flight("P2", "F2", "Canada", "UK", Date.valueOf("2023-01-15")),
      Flight("P1", "F3", "UK", "Germany", Date.valueOf("2023-02-10")),
      Flight("P3", "F4", "France", "UK", Date.valueOf("2023-02-20")),
      Flight("P1", "F5", "Germany", "Canada", Date.valueOf("2023-03-05"))
    ).toDS()

    // Sample passenger data
    val passengers: Dataset[Passenger] = Seq(
      Passenger("P1", "John", "Doe"),
      Passenger("P2", "Jane", "Doe"),
      Passenger("P3", "Mike", "Ross")
    ).toDS()

    // Expected result
    val expectedResult = Seq(
      ("P1", 3L, "John", "Doe"),
      ("P3", 1L, "Mike", "Ross"),
      ("P2", 1L, "Jane", "Doe")
    ).toDF("Passenger ID", "Number of Flights", "First Name", "Last Name")

    // Run the function under test
    val result = FlightDataAnalysis.answerQuestion2(flights, passengers)(spark)

    // Check the results
    assertDataFrameEquals(result, expectedResult)
  }

   /**
   * Tests for answerQuestion3
   */
  test("answerQuestion3 should calculate longest run of countries without visiting the UK") {
    // Sample flight data
    val flights: Dataset[Flight] = Seq(
      Flight("P1", "F1", "USA", "UK", Date.valueOf("2023-01-01")),
      Flight("P1", "F2", "UK", "Germany", Date.valueOf("2023-01-15")),
      Flight("P1", "F3", "Germany", "Canada", Date.valueOf("2023-02-10")),
      Flight("P1", "F4", "Canada", "France", Date.valueOf("2023-02-20")),
      Flight("P1", "F5", "France", "UK", Date.valueOf("2023-03-05")),
      Flight("P1", "F6", "UK", "USA", Date.valueOf("2023-03-10")),
      Flight("P2", "F7", "USA", "Canada", Date.valueOf("2023-01-01")),
      Flight("P2", "F8", "Canada", "Germany", Date.valueOf("2023-01-10")),
      Flight("P2", "F9", "Germany", "UK", Date.valueOf("2023-01-20")),
      Flight("P2", "F10", "UK", "France", Date.valueOf("2023-02-01"))
    ).toDS()

    // Expected result
    val expectedResult = Seq(
      ("P1", 3),
      ("P2", 2)
    ).toDF("Passenger ID", "Longest Run")

    // Run the function under test
    val result = FlightDataAnalysis.answerQuestion3(flights)(spark)

    // Check the results
    assertDataFrameEquals(result, expectedResult)
  }

   /**
   * Tests for answerQuestion4
   */
  test("answerQuestion4 should find passenger pairs with more than 3 flights together") {
    // Sample flight data
    val flights: Dataset[Flight] = Seq(
      Flight("P1", "F1", "USA", "UK", Date.valueOf("2023-01-01")),
      Flight("P2", "F1", "USA", "UK", Date.valueOf("2023-01-01")),
      Flight("P1", "F2", "UK", "Germany", Date.valueOf("2023-02-15")),
      Flight("P2", "F2", "UK", "Germany", Date.valueOf("2023-02-15")),
      Flight("P1", "F3", "Germany", "Canada", Date.valueOf("2023-02-10")),
      Flight("P2", "F3", "Germany", "Canada", Date.valueOf("2023-02-10")),
      Flight("P1", "F4", "Canada", "France", Date.valueOf("2023-02-20")),
      Flight("P2", "F4", "Canada", "France", Date.valueOf("2023-02-20")),
      Flight("P3", "F5", "France", "UK", Date.valueOf("2023-03-05")),
      Flight("P4", "F6", "UK", "USA", Date.valueOf("2023-03-10"))
    ).toDS()

    // Expected result
    val expectedResult = Seq(
      (PassengerPair("P1", "P2"), 4L)
    ).toDF("Passenger Pair", "Number of Flights Together")

    // Run the function under test
    val result = FlightDataAnalysis.answerQuestion4(flights)(spark)

    // Check the results
    assertDataFrameEquals(result.toDF(), expectedResult)
  }

   /**
   * Tests for answerQuestion5
   */
  test("answerQuestion5 should find passenger pairs that flew together at least N times within a date range") {
    // Sample flight data
    val flights: Dataset[Flight] = Seq(
      Flight("P1", "F1", "USA", "UK", Date.valueOf("2023-01-01")),
      Flight("P2", "F1", "USA", "UK", Date.valueOf("2023-01-01")),
      Flight("P1", "F2", "UK", "Germany", Date.valueOf("2023-02-15")),
      Flight("P2", "F2", "UK", "Germany", Date.valueOf("2023-02-15")),
      Flight("P1", "F3", "Germany", "Canada", Date.valueOf("2023-02-10")),
      Flight("P2", "F3", "Germany", "Canada", Date.valueOf("2023-02-10")),
      Flight("P1", "F4", "Canada", "France", Date.valueOf("2023-02-20")),
      Flight("P2", "F4", "Canada", "France", Date.valueOf("2023-02-20")),
      Flight("P3", "F5", "France", "UK", Date.valueOf("2023-03-05")),
      Flight("P4", "F6", "UK", "USA", Date.valueOf("2023-03-10"))
    ).toDS()

    // Parameters for answerQuestion5
    val atLeastNTimes = 3
    val from = Date.valueOf("2023-01-01")
    val to = Date.valueOf("2023-03-01")

    // Expected result
    val expectedResult = Seq(
      ("P1", "P2", 4, "2023-01-01", "2023-03-01")
    ).toDF("passenger1Id", "passenger2Id", "flightsTogether", "From", "To")

    // Run the function under test
    val result = FlightDataAnalysis.answerQuestion5(flights, atLeastNTimes, from, to)(spark)

    // Check the results
    assertDataFrameEquals(result, expectedResult)
  }

  def assertDataFrameEquals(result: DataFrame, expected: DataFrame): Unit = {
    assert(result.collect().toSet == expected.collect().toSet)
  }
}


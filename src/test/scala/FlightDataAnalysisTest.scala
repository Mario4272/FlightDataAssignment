import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.sql.Date
import org.apache.spark.sql.types.{StructType, StructField, StringType, DateType, LongType}

class FlightDataAnalysisTest extends AnyFunSuite with Matchers {
  val spark: SparkSession = SparkSession.builder()
    .appName("Flight Data Analysis Tests")
    .master("local[*]")
    .getOrCreate()

  import FlightDataAnalysis._
  import spark.implicits._

  var flights: Dataset[Flight] = _

  /**
   * Tests for answerQuestion1
   */
  test("answerQuestion1 answerQuestion1 method to calculate the total number of flights for each month") {
    // Sample dataset of flights
    val flightsData = Seq(
      Flight("1", "1001", "A", "B", Date.valueOf("2023-01-15")),
      Flight("2", "1002", "B", "C", Date.valueOf("2023-01-20")),
      Flight("3", "1003", "C", "D", Date.valueOf("2023-02-10")),
      Flight("4", "1004", "D", "E", Date.valueOf("2023-03-25")),
      Flight("5", "1005", "E", "F", Date.valueOf("2023-03-30")),
      Flight("6", "1006", "F", "G", Date.valueOf("2023-03-31"))
    )

    // Convert the sample data to a Dataset
    val flightsDS = flightsData.toDS()

    // Call the answerQuestion1 method to calculate the total number of flights for each month
    val result = answerQuestion1(flightsDS)(spark)

    // Define the expected output as a List of Rows
    val expectedData = Seq(
      Row(1, 2L),
      Row(2, 1L),
      Row(3, 3L)
    )

    // Define the expected schema
    val expectedSchema = StructType(List(
      StructField("Month", IntegerType, nullable = false),
      StructField("Number of Flights", LongType, nullable = false)
    ))

    // Convert the expected data and schema to a DataFrame
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)

    // Compare the result DataFrame with the expected DataFrame
    assert(result.collect().sameElements(expectedDF.collect()))
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
    val flightsData = Seq(
      Flight("1", "F001", "A", "B", Date.valueOf("2023-01-01")),
      Flight("2", "F002", "B", "C", Date.valueOf("2023-01-02")),
      Flight("1", "F003", "C", "D", Date.valueOf("2023-01-03")),
      Flight("3", "F004", "D", "E", Date.valueOf("2023-01-04")),
      Flight("1", "F005", "E", "F", Date.valueOf("2023-01-05"))
    )

    val passengersData = Seq(
      Passenger("1", "John", "Doe"),
      Passenger("2", "Jane", "Doe"),
      Passenger("3", "Sam", "Smith")
    )

    val flightsDS = flightsData.toDS()
    val passengersDS = passengersData.toDS()

    val result = answerQuestion3(flightsDS, passengersDS)(spark)

    val expectedData = Seq(
      Row("1", 3L, "John", "Doe"),
      Row("2", 1L, "Jane", "Doe"),
      Row("3", 1L, "Sam", "Smith")
    )

    val expectedSchema = StructType(List(
      StructField("Passenger ID", StringType, nullable = false),
      StructField("Number of Flights", LongType, nullable = false),
      StructField("First Name", StringType, nullable = false),
      StructField("Last Name", StringType, nullable = false)
    ))

    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)

    assert(result.collect().sameElements(expectedDF.collect()))
  }

  /**
   * Tests for answerQuestion3
   */

  test("answerQuestion3 Find the > # of countries") {
    val flightsData = Seq(
      Flight("1", "F001", "US", "UK", Date.valueOf("2023-01-01")),
      Flight("1", "F002", "UK", "FR", Date.valueOf("2023-01-02")),
      Flight("1", "F003", "FR", "US", Date.valueOf("2023-01-03")),
      Flight("1", "F004", "US", "CN", Date.valueOf("2023-01-04")),
      Flight("2", "F005", "CN", "DE", Date.valueOf("2023-01-05")),
      Flight("2", "F006", "DE", "UK", Date.valueOf("2023-01-06")),
      Flight("2", "F007", "UK", "US", Date.valueOf("2023-01-07")),
      Flight("2", "F008", "US", "CN", Date.valueOf("2023-01-08")),
      Flight("3", "F009", "CN", "US", Date.valueOf("2023-01-09")),
      Flight("3", "F010", "US", "UK", Date.valueOf("2023-01-10")),
      Flight("3", "F011", "UK", "CN", Date.valueOf("2023-01-11")),
      Flight("3", "F012", "CN", "DE", Date.valueOf("2023-01-12")),
      Flight("3", "F013", "DE", "US", Date.valueOf("2023-01-13")),
      Flight("3", "F014", "US", "FR", Date.valueOf("2023-01-14")),
      Flight("3", "F015", "FR", "UK", Date.valueOf("2023-01-15"))
    )

    val flightsDS = flightsData.toDS()

    val result = answerQuestion4(flightsDS)(spark)

    val expectedData = Seq(
      Row("1", 3),
      Row("2", 2),
      Row("3", 4)
    )

    val expectedSchema = StructType(List(
      StructField("Passenger ID", StringType, nullable = true),
      StructField("Longest Run", IntegerType, nullable = false)
    ))

    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)

    // Sort before comparison
    assert(result.sort("Passenger ID").collect().sameElements(expectedDF.sort("Passenger ID").collect()))
  }

  /**
   * Tests for answerQuestion4
   */
  val flightsSchema: StructType = StructType(Array(
    StructField("passengerId", StringType, nullable = true),
    StructField("flightId", StringType, nullable = true),
    StructField("from", StringType, nullable = true),
    StructField("to", StringType, nullable = true),
    StructField("date", DateType, nullable = true)
  ))

  val passengerPairsSchema: StructType = StructType(Array(
    StructField("passenger1Id", StringType, nullable = true),
    StructField("passenger2Id", StringType, nullable = true),
    StructField("numberOfFlightsTogether", LongType, nullable = true)
  ))

  test("Passengers with more than 3 flights together are returned correctly") {
    val flightData = Seq(
      Flight("1", "A", "X", "Y", Date.valueOf("2023-01-01")),
      Flight("2", "A", "X", "Y", Date.valueOf("2023-01-01")),
      Flight("1", "B", "Y", "Z", Date.valueOf("2023-01-02")),
      Flight("2", "B", "Y", "Z", Date.valueOf("2023-01-02")),
      Flight("1", "C", "Z", "X", Date.valueOf("2023-01-03")),
      Flight("2", "C", "Z", "X", Date.valueOf("2023-01-03")),
      Flight("1", "D", "X", "Z", Date.valueOf("2023-01-04")),
      Flight("2", "D", "X", "Z", Date.valueOf("2023-01-04")),
      Flight("3", "A", "X", "Y", Date.valueOf("2023-01-01"))
    ).toDS()

    val result = answerQuestion5(flightData)(spark)

    val expectedData = Seq(
      PassengerPair("1", "2", 4)
    ).toDS()

    assertDataFrameEquals(result, expectedData)
  }

  test("Passengers with 3 or fewer flights together are not returned") {
    val flightData = Seq(
      Flight("1", "A", "X", "Y", Date.valueOf("2023-01-01")),
      Flight("2", "A", "X", "Y", Date.valueOf("2023-01-01")),
      Flight("1", "B", "Y", "Z", Date.valueOf("2023-01-02")),
      Flight("2", "B", "Y", "Z", Date.valueOf("2023-01-02")),
      Flight("1", "C", "Z", "X", Date.valueOf("2023-01-03")),
      Flight("3", "C", "Z", "X", Date.valueOf("2023-01-03")),
      Flight("1", "D", "X", "Z", Date.valueOf("2023-01-04")),
      Flight("3", "D", "X", "Z", Date.valueOf("2023-01-04")),
      Flight("2", "E", "Z", "Y", Date.valueOf("2023-01-05")),
      Flight("3", "E", "Z", "Y", Date.valueOf("2023-01-05"))
    ).toDS()

    val result = answerQuestion5(flightData)(spark)

    assert(result.isEmpty, "Dataframe should be empty")
  }

  test("Test with empty dataset returns empty result") {
    val flightData = Seq.empty[Flight].toDS()

    val result = answerQuestion4(flightData)(spark)

    assert(result.isEmpty, "Dataframe should be empty")
  }

  // Additional test cases can be added here to cover more scenarios

  // Helper function to compare DataFrames
  def assertDataFrameEquals(actual: Dataset[PassengerPair], expected: Dataset[PassengerPair]): Unit = {
    assert(actual.except(expected).isEmpty, "DataFrames are not equal")
    assert(expected.except(actual).isEmpty, "DataFrames are not equal")
  }

  /*/**
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
    assertDataFrameEquals(result.orderBy("passenger1Id", "passenger2Id"), expectedResult.orderBy("passenger1Id", "passenger2Id"))

    def assertDataFrameEquals(result: DataFrame, expected: DataFrame): Unit = {
      assert(result.collect().toSet == expected.collect().toSet)
    }
  }*/
}
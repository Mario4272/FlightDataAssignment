import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.sql.Date
import Implicits._
/**
 * Object to analyze flight data.
 */
object FlightDataAnalysis {
val outPutFolder = "output"
  /** Case class representing a flight. */
  case class Flight(passengerId: String, flightId: String, from: String, to: String, date: Date)
  /** Case class representing a Passenger. */
  case class Passenger(passengerId: String, firstName: String, lastName: String)
  /** Case class representing a pair of flights. */
  case class FlightPair(passenger1Id: String, passenger2Id: String, flightId: String, date: Date)
/**Case class representing pairs of passengers*/
case class PassengerPair(`Passenger 1 ID`: String, `Passenger 2 ID`: String, `Number of Flights Together`: Long)




  /**
   * Answers question 1 & 11: Counts the number of flights per month.
   *
   * @param flights The dataset of flights.
   * @param spark The Spark session.
   * @return A DataFrame with two columns: Month and Number of Flights.
   */
  def answerQuestion1(flights: Dataset[Flight])(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Calculate the total number of flights for each month
    val result = flights
      .groupBy(month($"date").as("Month")) // Group by month and rename the column to "Month"
      .agg(count("*").as("Number of Flights")) // Count the number of flights in each group
      .sort("Month") // Sort the results by month

    // Write the result to a CSV file
    val fn = "Q1AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    result.writeToCsv(fullPath)

    // Return the result
    result
  }

  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.{Dataset, SparkSession}
  import org.apache.spark.sql.functions._
  import java.sql.Date

  def answerQuestion11(flights: Dataset[Flight])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Transform Dataset[Flight] to RDD, map it to extract the month and use reduceByKey to count
    val monthCounts: RDD[(Int, Long)] = flights.rdd
      .map(flight => (flight.date.toLocalDate.getMonthValue, 1L))
      .reduceByKey(_ + _)
      .sortByKey()

    // Convert RDD back to DataFrame for easier output purposes
    val result = monthCounts.toDF("Month", "Number of Flights")

    // Write the result to a CSV file
    val fn = "Q1AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    result.writeToCsv(fullPath) // Assuming writeToCsv is an extension method available for DataFrame
    result
  }
  /**
   * Answers question 2: Gets the top 100 passengers who have flown the most.
   *
   * @param flights The dataset of flights.
   * @param passengers The dataset of passengers.
   * @param spark The Spark session.
   * @return A DataFrame with the passenger details and the number of flights they've taken.
   */
  def answerQuestion2(flights: Dataset[Flight], passengers: Dataset[Passenger])(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Calculate the number of flights for each passenger, join with passengers dataset, and sort the results
    val result = flights
      .groupBy($"passengerId")
      .agg(count("*").as("Number of Flights"))
      .join(passengers, "passengerId")
      .select($"passengerId", $"Number of Flights", $"firstName", $"lastName")
      .toDF("Passenger ID", "Number of Flights", "First Name", "Last Name")
      .sort(desc("Number of Flights"), asc("Passenger ID"))
      .limit(100)

    // Write the result to a CSV file
    val fn = "Q2AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    result.writeToCsv(fullPath)

    // Return the result
    result
  }

  /**
   * Computes the longest run of countries visited without visiting the UK.
   *
   * @param countries A sequence of country codes.
   * @return The length of the longest run.
   */
  def longestRun(countries: Seq[String]): Int = {
    countries.foldLeft((0, 0)) { case ((currentRun, maxRun), country) =>
      if (country == "UK") (0, maxRun)
      else (currentRun + 1, Math.max(currentRun + 1, maxRun))
    }._2
  }

  // UDF Call
  val longestRunUDF: UserDefinedFunction = udf((countries: Seq[String]) => longestRun(countries))

  /**
   * Answers question 3: Find the greatest number of countries a passenger has been in without being in the UK.
   * For example, if the countries a passenger was in were:
   * UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.
   *
   * @param flights The dataset of flights.
   * @param spark   The implicit Spark session.
   * @return A Dataset of Passenger Pairs and the number of flights they've flown together.
   */
  def answerQuestion3(flights: Dataset[Flight])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy($"passengerId").orderBy($"date")

    val result = flights
      .withColumn("countries", collect_list($"to").over(windowSpec))
      .groupBy($"passengerId")
      .agg(max(longestRunUDF($"countries")).as("Longest Run"))
      .select($"passengerId".as("Passenger ID"), $"Longest Run")
      .sort(desc("Longest Run"))

    val fn = "Q3AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    result.writeToCsv(fullPath)

    //Return result
    result
  }

  /**
   * Answers question 4: Finds passenger pairs that have flown together more than 3 times.
   *
   * @param flights The dataset of flights.
   * @param spark   The implicit Spark session.
   * @return A Dataset of Passenger Pairs and the number of flights they've flown together.
   */
  def answerQuestion4(flights: Dataset[Flight])(implicit spark: SparkSession): Dataset[PassengerPair] = {
    import spark.implicits._

    // Generate all possible pairs of passengers for each flight
    val passengerPairs = flights
      .as("f1")
      .join(flights.as("f2"), $"f1.flightId" === $"f2.flightId" && $"f1.passengerId" < $"f2.passengerId")
      .select($"f1.passengerId".alias("passenger1Id"), $"f2.passengerId".alias("passenger2Id"))

    // Count the number of flights together for each passenger pair
    val flightCounts = passengerPairs
      .groupBy("passenger1Id", "passenger2Id")
      .count()
      .withColumnRenamed("count", "Number of Flights Together")
      .withColumnRenamed("passenger1Id", "Passenger 1 ID")
      .withColumnRenamed("passenger2Id", "Passenger 2 ID")
      .filter($"Number of Flights Together" > 3)
      .as[PassengerPair]

    val fn = "Q4AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    flightCounts.coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .mode("overwrite")
      .csv(fullPath)
    // Return the result
    flightCounts
  }

  /**
   * Answers question 5 & 55: Finds passenger pairs that have flown together at least a certain number of times within a date range.
   *
   * @param flights       The dataset of flights.
   * @param atLeastNTimes The minimum number of times the passenger pairs should have flown together.
   * @param from          The start date of the date range.
   * @param to            The end date of the date range.
   * @param spark         The Spark session.
   * @return A DataFrame with passenger pair details, number of flights together, and the date range.
   */
  def answerQuestion5(flights: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val outputPath = "output/path"

    // Filter flights within the given date range
    val filteredFlights = flights.filter($"date" >= from && $"date" <= to)

    // For each flight, create a list of all possible pairs of passengers
    val passengerPairs = filteredFlights
      .groupBy($"flightId", $"date")
      .agg(collect_list($"passengerId").as("passengerIds"))
      .as[(String, Date, Seq[String])]
      .flatMap { case (flightId, date, passengerIds) =>
        for {
          passenger1Id <- passengerIds
          passenger2Id <- passengerIds
          if passenger1Id < passenger2Id
        } yield FlightPair(passenger1Id, passenger2Id, flightId, date)
      }

    // Count the occurrences of each passenger pair in the flights
    val flightTogetherCounts = passengerPairs
      .groupBy($"passenger1Id", $"passenger2Id")
      .agg(countDistinct($"flightId").as("Number of Flights Together"))
      .filter($"Number of Flights Together" >= atLeastNTimes)

    // Add a column with the date range and rename columns
    val result = flightTogetherCounts
      .withColumnRenamed("passenger1Id", "Passenger 1 ID")
      .withColumnRenamed("passenger2Id", "Passenger 2 ID")
      .withColumn("From", lit(from))
      .withColumn("To", lit(to))
      .as[PassengerPair]
      .toDF()

    // Write out to Answer File
    val fn = "Q5AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    result.writeToCsv(fullPath)

    // Return the result
    result
  }

  def answerQuestion55(flights: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Filter the Dataset first using RDD operations
    val filteredFlightsRDD = flights.rdd
      .filter(flight => !flight.date.before(from) && !flight.date.after(to))

    // Generate passenger pairs and count flights together
    val passengerPairsRDD = filteredFlightsRDD
      .groupBy(flight => flight.flightId)
      .flatMap {
        case (_, flightGroup) =>
          val passengers = flightGroup.map(_.passengerId).toList
          for {
            a <- passengers
            b <- passengers if a < b
          } yield ((a, b), 1)
      }
      .reduceByKey(_ + _)
      .filter(_._2 >= atLeastNTimes)
      .map {
        case ((passenger1Id, passenger2Id), count) =>
          PassengerPair(passenger1Id, passenger2Id, count)
      }

    // Convert RDD[PassengerPair] to DataFrame for easier output purposes
    val result = passengerPairsRDD.toDF()

    // Write the result to a CSV file
    val fn = "Q5AnswerFile"
    val fullPath = s"$outPutFolder/$fn"
    result.writeToCsv(fullPath)
    result
  }
}

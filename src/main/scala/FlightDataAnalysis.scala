import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Date
/**
 * Object to analyze flight data.
 */
object FlightDataAnalysis {

  /** Case class representing a flight. */
  case class Flight(passengerId: String, flightId: String, from: String, to: String, date: Date)
  /** Case class representing a Passenger. */
  case class Passenger(passengerId: String, firstName: String, lastName: String)
  /** Case class representing a pair of flights. */
  case class FlightPair(passenger1Id: String, passenger2Id: String, flightId: String, date: Date)
  /** Case class representing a pair of passengers. */
  case class PassengerPair(passenger1Id: String, passenger2Id: String)


  /**
   * Answers question 1: Counts the number of flights per month.
   *
   * @param flights The dataset of flights.
   * @param spark The Spark session.
   * @return A DataFrame with two columns: Month and Number of Flights.
   */
  def answerQuestion1(flights: Dataset[Flight])(spark: SparkSession): DataFrame = {
    import spark.implicits._
    flights
      .groupByKey(f => f.date.toLocalDate.getMonthValue)
      .count()
      .toDF("Month", "Number of Flights")
      .sort("Month")
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
    flights
      .groupBy($"passengerId")
      .agg(count("*").as("Number of Flights"))
      .join(passengers, "passengerId")
      .select($"passengerId", $"Number of Flights", $"firstName", $"lastName")
      .toDF("Passenger ID", "Number of Flights", "First Name", "Last Name")
      .sort(desc("Number of Flights"), asc("Passenger ID"))
      .limit(100)
  }
  /**
   * Computes the longest run of countries visited without visiting the UK.
   *
   * @param countries A sequence of country codes.
   * @return The length of the longest run.
   */  def longestRun(countries: Seq[String]): Int = {
    countries.foldLeft((0, 0)) { case ((currentRun, maxRun), country) =>
      if (country == "UK") (0, maxRun)
      else (currentRun + 1, Math.max(currentRun + 1, maxRun))
    }._2
  }

  /**
   * Registers longestRun function as a Spark UDF.
   *
   * @return The UserDefinedFunction.
   */
  def longestRunUDF: UserDefinedFunction = udf(longestRun _)
  /**
   * Answers question 3: Finds the longest run of countries visited without visiting the UK for each passenger.
   *
   * @param flights The dataset of flights.
   * @param spark The Spark session.
   * @return A DataFrame with Passenger ID and their Longest Run.
   */
  def answerQuestion3(flights: Dataset[Flight])(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy($"passengerId").orderBy($"date")


    flights
      .withColumn("countries", collect_list($"to").over(windowSpec))
      .groupBy($"passengerId")
      .agg(max(longestRunUDF($"countries")).as("Longest Run"))
      .sort(desc("Longest Run"))
      .toDF("Passenger ID", "Longest Run")
  }
  /**
   * Answers question 4: Finds passenger pairs that have flown together more than 3 times.
   *
   * @param flights The dataset of flights.
   * @param spark The implicit Spark session.
   * @return A Dataset of Passenger Pairs and the number of flights they've flown together.
   */
  def answerQuestion4(flights: Dataset[Flight])(implicit spark: SparkSession): Dataset[(PassengerPair, Long)] = {
    import spark.implicits._

    val flightPairs = flights
      .as("f1")
      .join(flights.as("f2"),
        $"f1.flightId" === $"f2.flightId" &&
          $"f1.passengerId" < $"f2.passengerId")
      .select($"f1.passengerId".as("passenger1Id"), $"f2.passengerId".as("passenger2Id"))
      .as[PassengerPair]

    flightPairs
      .groupByKey(identity)
      .count()
      .filter(_._2 > 3)
      .toDF("Passenger Pair", "Number of Flights Together")
      .sort(desc("Number of Flights Together"))
      .as[(PassengerPair, Long)]
  }

  /**
   * Answers question 5: Finds passenger pairs that have flown together at least a certain number of times within a date range.
   *
   * @param flights       The dataset of flights.
   * @param atLeastNTimes The minimum number of times the passenger pairs should have flown together.
   * @param from          The start date of the date range.
   * @param to            The end date of the date range.
   * @param spark         The Spark session.
   * @return A DataFrame with passenger pair details, number of flights together, and the date range.
   */
  def answerQuestion5(flights: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date)(spark: SparkSession): DataFrame = {
    import spark.implicits._

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
      .agg(countDistinct($"flightId").as("flightsTogether"))
      .filter($"flightsTogether" >= atLeastNTimes)

    // Add a column with the date range
    flightTogetherCounts
      .withColumn("From", lit(from.toString))
      .withColumn("To", lit(to.toString))
      .select($"passenger1Id", $"passenger2Id", $"flightsTogether", $"From", $"To")
  }
}

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Date

object FlightDataAnalysis {

  case class Flight(passengerId: String, flightId: String, from: String, to: String, date: Date)
  case class Passenger(passengerId: String, firstName: String, lastName: String)
  case class FlightPair(passenger1Id: String, passenger2Id: String, flightId: String, date: Date)
  case class PassengerPair(passenger1Id: String, passenger2Id: String)



  def answerQuestion1(flights: Dataset[Flight])(spark: SparkSession): DataFrame = {
    import spark.implicits._
    flights
      .groupByKey(f => f.date.toLocalDate.getMonthValue)
      .count()
      .toDF("Month", "Number of Flights")
      .sort("Month")
  }

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
  // Function to compute the longest run of countries without visiting the UK
  def longestRun(countries: Seq[String]): Int = {
    countries.foldLeft((0, 0)) { case ((currentRun, maxRun), country) =>
      if (country == "UK") (0, maxRun)
      else (currentRun + 1, Math.max(currentRun + 1, maxRun))
    }._2
  }

  // Registering the function as a Spark UDF
  def longestRunUDF: UserDefinedFunction = udf(longestRun _)

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

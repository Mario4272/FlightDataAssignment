import org.apache.spark.sql.SparkSession
import java.sql.Date
import scala.io.StdIn
import scala.io.StdIn._
/**
 * The Main object serves as the entry point for the Flight Data Analysis application.
 * It provides users with the ability to interactively choose and execute specific analysis tasks on flight data.
 *
 * The supported analysis tasks are based on the FlightDataAnalysis object.
 *
 * @example To run a specific analysis task, execute the application and follow the interactive prompts.
 */
object Main extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("Flight Data Analysis")
    .master("local[*]") // Remove this line when running on a real cluster
    .getOrCreate()

  import spark.implicits._

  val flightDataPath = "src/main/resources/flightData.csv"
  val passengerDataPath = "src/main/resources/passengers.csv"
  /**
   * Loads data from a CSV file into a Dataset.
   *
   * param path The path to the CSV file.
   * return A Dataset of FlightData/PassengerData case class instances.
   */
  val flightData = spark.read.option("header", "true").csv(flightDataPath).as[FlightDataAnalysis.Flight]
  val passengersData = spark.read.option("header", "true").csv(passengerDataPath).as[FlightDataAnalysis.Passenger]

  // Calling the interactive menu
  try {
    // Calling the interactive menu
    interactiveMenu()
  } finally {
    // Ensure that the Spark session is stopped properly.
    println("Stopping Spark session...")
    spark.stop()
    println("Spark session stopped.")
  }
  /**
   * Provides an interactive command-line interface for the user to select and execute analysis tasks.
   */

  def interactiveMenu(): Unit = {
    var continueMenu = true

    while (continueMenu) {
      println("Welcome to the Flight Data Analysis application!")
      println("Please choose an option:")
      println("1: Total number of flights for each month")
      println("2: Total number of flights for each month, No DataFrame")
      println("3: Top 100 most frequent flyers")
      println("4: Greatest number of countries a passenger has been without being in the UK")
      println("5: Passengers who have been on more than 3 flights together")
      println("6: Passengers who have been on more than N flights together within a date range")
      println("7: Passengers who have been on more than N flights together within a date range, No DataFrame")
      println("0: Exit")

      val userChoice = StdIn.readInt()

      userChoice match {
        case 1 => FlightDataAnalysis.answerQuestion1(flightData)(spark).show()
        case 2 =>
          val result2 = FlightDataAnalysis.answerQuestion2(flightData)(spark)
          result2.show() //Showcase RDD
        case 3 => FlightDataAnalysis.answerQuestion3(flightData, passengersData)(spark) show()
        case 4 => FlightDataAnalysis.answerQuestion4(flightData)(spark).show()
        case 5 => FlightDataAnalysis.answerQuestion5(flightData)(spark).show()
        case 6 =>
          println("Enter atLeastNTimes:")
          val atLeastNTimes = readInt()
          println("Enter from date (yyyy-MM-dd):")
          val from = Date.valueOf(readLine())
          println("Enter to date (yyyy-MM-dd):")
          val to = Date.valueOf(readLine())
          FlightDataAnalysis.answerQuestion6(flightData, atLeastNTimes, from, to)(spark).show()
        case 7 =>
          println("Enter atLeastNTimes:")
          val atLeastNTimes = readInt()
          println("Enter from date (yyyy-MM-dd):")
          val from = Date.valueOf(readLine())
          println("Enter to date (yyyy-MM-dd):")
          val to = Date.valueOf(readLine())
          val resultDF5 = FlightDataAnalysis.answerQuestion7(flightData, atLeastNTimes, from, to)(spark) // This should return a DataFrame
          resultDF5.show() //Showcase RDD1
        case 0 =>
          println("Exiting the application...")
          continueMenu = false
        case _ =>
          println("Invalid choice. Please enter a number between 0 and 7.")
      }
    }

    println("Thank you for using the Flight Data Analysis application!")

  }
  spark.stop()
}

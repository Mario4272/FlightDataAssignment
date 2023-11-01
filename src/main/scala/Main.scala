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
  interactiveMenu()
  /**
   * Provides an interactive command-line interface for the user to select and execute analysis tasks.
   */
  def interactiveMenu(): Unit = {
    var continueMenu = true

    while (continueMenu) {
      println("Welcome to the Flight Data Analysis application!")
      println("Please choose an option:")
      println("1: Total number of flights for each month")
      println("2: Top 100 most frequent flyers")
      println("3: Greatest number of countries a passenger has been without being in the UK")
      println("4: Passengers who have been on more than 3 flights together")
      println("5: Passengers who have been on more than N flights together within a date range")
      println("0: Exit")

      val userChoice = StdIn.readInt()

      userChoice match {
        case 1 => FlightDataAnalysis.answerQuestion1(flightData)(spark).show()
        case 2 => FlightDataAnalysis.answerQuestion2(flightData, passengersData)(spark) show()
        case 3 => FlightDataAnalysis.answerQuestion3(flightData)(spark).show()
        case 4 => FlightDataAnalysis.answerQuestion4(flightData)(spark).show()
        case 5 =>
          println("Enter atLeastNTimes:")
          val atLeastNTimes = readInt()
          println("Enter from date (yyyy-MM-dd):")
          val from = Date.valueOf(readLine())
          println("Enter to date (yyyy-MM-dd):")
          val to = Date.valueOf(readLine())
          FlightDataAnalysis.answerQuestion5(flightData, atLeastNTimes, from, to)(spark).show()
        case _ => println("Invalid choice. Please enter a number between 1 and 5.")
      }
    }

    println("Thank you for using the Flight Data Analysis application!")

  }
  spark.stop()
}

package example

import scala.io.Source
import scala.util.Try
import java.time.LocalDate
import java.time.format.DateTimeFormatter


case class FlightData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class PassengerData(passengerId: Int, firstName: String, lastName: String)


object Main {
  def main(args: Array[String]): Unit = {
//    Example
//    val ages = Seq(42, 61, 29, 64)
//    println(s"The oldest person is ${ages.max}")

//    Read flight and passenger data
    val flightData = readFlightData("src/main/resources/flightData.csv")
    val passengerData = readPassengerData("src/main/resources/passengers.csv")

//    print the first 5 lines
    println("First 5 Flight Data Records:")
    flightData.take(5).foreach(println)

    println("\nFirst 5 Passenger Data Records:")
    passengerData.take(5).foreach(println)

//    Answer for Q1
    val flightPerMonth = countFlightPerMonth(flightData)
    println("Month\tNumber of Flights")
    flightPerMonth.foreach {
      case (month,count) =>
        println(s"$month\t$count")
    }

//    Answer for Q2
    val topFlyers = findTopFlyers(flightData, passengerData)
    println("Passenger ID\tNumber of Flights\tFirst Name\tLast Name")
    topFlyers.foreach{
      case(passengerId, flightCount, firstName, lastName) =>
        println(s"$passengerId\t$flightCount\t$firstName\t$lastName")
    }
  }

  // Function to read flight data csv files
  def readFlightData(filePath: String): List[FlightData] = {
    var source: Option[Source] = None
    try {
      source = Some(Source.fromFile(filePath))
      source.get.getLines().drop(1).flatMap { line =>
        val cols = line.split(",").map(_.trim)
        Try(FlightData(cols(0).toInt, cols(1).toInt, cols(2), cols(3), cols(4))).toOption
      }.toList
    } finally {
      source.foreach(_.close())
    }
  }

  // Function to read passenger data csv files
  def readPassengerData(filePath: String): List[PassengerData] = {
    var source: Option[Source] = None
    try {
      source = Some(Source.fromFile(filePath))
      source.get.getLines().drop(1).flatMap { line =>
        val cols = line.split(",").map(_.trim)
        Try(PassengerData(cols(0).toInt, cols(1), cols(2))).toOption
      }.toList
    } finally {
      source.foreach(_.close())
    }
  }

//  Function to find the total number of flights for each month
  def countFlightPerMonth(flightData: List[FlightData]): List[(Int, Int)] = {
    flightData
      .flatMap{ data =>
        Try{
          // Extract month value
          val date = LocalDate.parse(data.date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          val month = date.getMonthValue
          month -> 1
        }.toOption
      }
      .groupBy(_._1) // Group by month
      .map { case (month, flights) => (month, flights.size) } // Count the number of flights
      .toList
      .sortBy(_._1) // Sort by month
  }

// Function to find the names of the 100 most frequent flyers
  def findTopFlyers(flightData: List[FlightData], passengerData: List[PassengerData]): List[(Int, Int, String, String)] = {
    // Count flight numbers per passenger
    val flightCounts = flightData
      .groupBy(_.passengerId)
      .mapValues(_.size)
      .toList
    // Sort by the flight number in descending order, and take top 100
    val top100 = flightCounts.sortBy(-_._2).take(100)

    val passengerMap = passengerData.map(p => p.passengerId -> (p.firstName, p.lastName)).toMap
    // Join flight counts with passenger names
    top100.flatMap { case(passengerId, count) =>
    passengerMap.get(passengerId) match {
      case Some((firstName, lastName)) => Some((passengerId, count, firstName, lastName))
    }
    }
  }


}
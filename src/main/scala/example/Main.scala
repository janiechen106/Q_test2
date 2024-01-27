package example

import scala.io.Source
import scala.util.Try
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io.PrintWriter

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
      case (month, count) =>
        println(s"$month\t$count")
    }

    //    Write Q1 to CSV
    writeQ1CSV(flightPerMonth, "src/main/resources/q1answer.csv")


    //    Answer for Q2
    val topFlyers = findTopFlyers(flightData, passengerData)
    println("Passenger ID\tNumber of Flights\tFirst Name\tLast Name")
    topFlyers.foreach {
      case (passengerId, flightCount, firstName, lastName) =>
        println(s"$passengerId\t$flightCount\t$firstName\t$lastName")
    }

    //    Write Q2 to CSV
    writeQ2CSV(topFlyers, "src/main/resources/q2answer.csv")


    //    Answer for Q3
    val longestTrip = findLongestTrip(flightData)
    println("Passenger ID\tLongest Run")
    longestTrip.foreach { case (passengerId, longestStops) =>
      println(s"$passengerId\t$longestStops")
    }

    //    Write Q3 to CSV
    writeQ3CSV(longestTrip, "src/main/resources/q3answer.csv")


    //       Answer for Q4
    val frequentFlyerPairs = findFrequentPairs(flightData)
    println("Passenger 1 ID\tPassenger 2 ID\tNumber of flights together")
    frequentFlyerPairs.foreach {
      case ((passenger1Id, passenger2Id), count) =>
        println(s"$passenger1Id\t$passenger2Id\t$count")
    }

    //    Write Q4 to CSV
    writeQ4CSV(frequentFlyerPairs, "src/main/resources/q4answer.csv")


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

//  Function for Q1, find the total number of flights for each month
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

  // Function for writing Q1 answer to csv
  def writeQ1CSV(data: List[(Int, Int)], filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      writer.println("Month,Number of Flights")
      data.foreach { case (month, count) =>
        writer.println(s"$month,$count")
      }
    } finally {
      writer.close()
    }
  }

  // Function for Q2, find the names of the 100 most frequent flyers
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

  // Function for writing Q2 answer to csv
  def writeQ2CSV(data: List[(Int, Int, String, String)], filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      writer.println("Passenger ID,Number of Flights,First Name,Last Name")
      data.foreach { case (passengerId, flightCount, firstName, lastName) =>
        writer.println(s"$passengerId,$flightCount,$firstName,$lastName")
    }
  } finally {
    writer.close()}
  }

  // Function for Q3, find the greatest number of countries a passenger has been in without being in the UK.
  def findLongestTrip(flightData: List[FlightData]): List[(Int, Int)] = {
    flightData
      .groupBy(_.passengerId)
      .map { case (passengerId, flights) =>
        // List all the countries that each passenger has visited
        val countriesVisited = flights.flatMap(flight => List(flight.from, flight.to))
        // Split the list into sublists whenever "UK" is encountered
        val countriesWithoutUK = countriesVisited.mkString("->").split("->UK->").toList
        // Find the longest
        val longestStops = countriesWithoutUK.map(_.split("->").distinct.length).max
        (passengerId, longestStops)
      }
      .toList
      .sortBy(-_._2)
  }

  // Function for writing Q3 answer to csv
  def writeQ3CSV(data: List[(Int, Int)], filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      writer.println("Passenger ID,Longest Run")
      data.foreach { case (passengerId, longestStops) =>
        writer.println(s"$passengerId,$longestStops")}
    } finally {
      writer.close()
    }
  }


  //  Function for Q4, find the passengers who have been on more than 3 flights together.
  def findFrequentPairs(flightData: List[FlightData]): List[((Int, Int), Int)] = {
    val eachFlightPassengers = flightData
      .groupBy(flight => (flight.flightId, flight.date))
      .map { case (_, groupedFlights) => groupedFlights.map(_.passengerId).distinct }

    val passengerPairs = eachFlightPassengers
      .flatMap { passengers =>
        passengers.combinations(2).map {
          case List(passenger1Id, passenger2Id) => (passenger1Id, passenger2Id)
        }
      }

    val frequentPairsCount = passengerPairs
      .groupBy(identity)
      .map { case (pair, occurrences) => (pair, occurrences.size) }
      .filter(_._2 > 3)
      .toList
      .sortBy(-_._2)

    frequentPairsCount
  }

  // Function for writing Q4 answer to csv
  def writeQ4CSV(data: List[((Int, Int), Int)], filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      writer.println("Passenger 1 ID,Passenger 2 ID,Number of flights together")
      data.foreach { case ((passenger1Id, passenger2Id), count) =>
        writer.println(s"$passenger1Id,$passenger2Id,$count")
      }
    } finally {
      writer.close()
    }
  }


}

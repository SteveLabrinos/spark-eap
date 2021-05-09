package exercise2

import org.apache.spark.SparkContext

object ChessGames {
  def parseLine(line: String): (String, String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the two players and combine them after sorting
    val players = List(fields(8), fields(10))
      .sorted
      .mkString("_")
    // Get the moves field for the second report
    val moves = fields(12)
    // Create a tuple that with the key and 1 as the counter
    (players, moves)
  }

  def main(args: Array[String]): Unit = {
    // Create a SparkContext using the local machine
    val sc = new SparkContext("local[*]", "WordCount")
    // Load each line of the book into an RDD
    val input = sc.textFile("data/games.csv")
      // Get the tuple (players, 1) for every player couple
      .map(parseLine)

    // Get the occurrences for each player couple
    val playerCouples = input
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      // Getting the couples with more than 5 occurrences
      .filter(_._2 > 5)
      // Flip the key - value to sort by key
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      // Collecting the RDD results from the cluster (different cores for local execution)
      .collect()

    // Get all the different moves as keys with flat map
    val moves = input
      .flatMap(_._2.split(" "))
      // Get a tuple with key and 1 as the value for each move
      .map((_, 1))
      .reduceByKey(_ + _)
      // Flip the key - value to sort by key
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      // Collecting the RDD results from the cluster (different cores for local execution)
      .collect()

    // Print the results
    // All the couples that compete more than 5 times
    println("Player couples with more than 5 games played as opponents")
    println("=========================================================")
    for (playerCouple <- playerCouples) {
      val couple = playerCouple._2
      val occurrences = playerCouple._1
      println(s"$couple: $occurrences")
    }

    // The first 5 moves with the most occurrences
    println("\n\nTop 5 most used moves in chess games")
    println("====================================")

    for (move <- moves.take(5)) {
      val moveId = move._2
      val occurrences = move._1
      println(s"$moveId: $occurrences")
    }
  }

}

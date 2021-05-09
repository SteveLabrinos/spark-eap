package exercise1

import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    // Create a SparkContext using the local machine
    val sc = new SparkContext("local[*]", "WordCount")
    // Load each line of the book into an RDD
    val input = sc.textFile("data/SherlockHolmes.txt")
    // Split using a regular expression that extracts words into lines with flatMap
    val lowercaseWords = input
      .flatMap(_.split("\\W+"))
      // Normalize everything to lowercase
      .map(_.toLowerCase())

    // Step 1
    // Calculating total word count and length of every word
    // Mapping each word/line in a tuple (word_length, 1)
    val wordsLengthAndCount = lowercaseWords
      .map(x => (x.length, 1))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    // Getting the average word length of the file
    val wordAvg: Float = wordsLengthAndCount._1 / wordsLengthAndCount._2.toFloat

    // Step 2
    // Filter the words with length less than the wordAvg
    val wordCounts = lowercaseWords
      .filter(_.length.toFloat >= wordAvg)
      // Count of the occurrences of each word
      .map((_, 1))
      .reduceByKey(_ + _)
      // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      // Collecting the RDD results from the cluster (different cores for local execution)
      .collect()

    // Printing the results of the exercise
    println("Total words found: " + wordsLengthAndCount._2)
    val formatAverage = f"$wordAvg%.2f"
    println(s"Average length of all the words: $formatAverage")

    // Print the results the first 5 results (count, word).
    for (result <- wordCounts.take(5)) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}

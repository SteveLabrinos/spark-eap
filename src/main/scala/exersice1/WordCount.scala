package exersice1

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    println("hello world")

    val ss = SparkSession
      .builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()

    val sc = ss.sparkContext

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    println(distData.sum())
  }

}

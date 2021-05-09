package exercise4

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions.{avg, desc, round, sum, count}


object SalesReport {

  // Define a class to implicitly convert the DataFrame to DataSet
  case class Transaction(
                          InvoiceNo: String,
                          StockCode: String,
                          Quantity: Int,
                          UnitPrice: Double,
                          CustomerId: Int,
                        )

  def main(args: Array[String]): Unit = {
    // Use new SparkSession
    val spark = SparkSession
      .builder
      .appName("SalesReport")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Transaction case class
    // class to infer the schema.
    import spark.implicits._
    val sales: Dataset[Transaction] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")
      .as[Transaction]

    // We start by declaring the anonymous function in Scala to calculate the turnover
    val calcTurnover: (Int, Double) => Double = (Quantity: Int, UnitPrice: Double) => {
      Quantity * UnitPrice
    }

    // Wrap the anonymous function  with a udf
    val salesTurnoverUDF = udf(calcTurnover)

    // Add a movieTitle column using our new udf
    val salesWithTurnover = sales
      .withColumn("Turnover", salesTurnoverUDF(col("Quantity"), col("UnitPrice")))

    // Top 5 invoices by summing up the turnover for each invoice
    println("Top 5 invoices with the largest turnovers")
    salesWithTurnover
      .groupBy("InvoiceNo")
      .agg(round(sum("Turnover"), 2).alias("TotalTurnover"))
      .orderBy(desc("TotalTurnover"))
      .show(5)

    // Top 5 products by summing turnover for each product
    // Using turnover and not quantity because unit price can change for a product
    println("Top 5 products in terms of profit")
    salesWithTurnover
      .groupBy("StockCode")
      .agg(round(sum("Turnover"), 2).alias("TotalTurnover"))
      .orderBy(desc("TotalTurnover"))
      .show(5)

    // Top 5 products by counting the appearances in different invoices
    println("Top 5 products in terms of appearances")
    salesWithTurnover
      .groupBy("StockCode")
      .agg(count("*").alias("Appearances"))
      .orderBy(desc("Appearances"))
      .show(5)

    // Average Quantity and Turnover for each invoice
    println("Average Quantity and Turnover of invoices")
    salesWithTurnover
      .groupBy("InvoiceNo")
      .agg(round(avg("Quantity"), 2).alias("AverageProducts"),
        round(avg("Turnover"), 2).alias("AverageTurnover"))
      .orderBy("InvoiceNo")
      // Printing first 20 results. Use salesWithTurnover.count.toInt
      // as a parameter to fetch all the results
      .show()

    // Best 5 customers by summing turnover for each customer
    // Rejecting transaction with no customerID
    println("Top 5 spending customers")
    salesWithTurnover
      .filter($"CustomerID".isNotNull)
      .groupBy("CustomerID")
      .agg(round(sum("Turnover"), 2).alias("TotalAmountSpent"))
      .orderBy(desc("TotalAmountSpent"))
      .show(5)
  }

}

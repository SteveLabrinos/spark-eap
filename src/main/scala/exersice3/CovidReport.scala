package exersice3

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, desc, round, sum}

object CovidReport {
  // Define a class to implicitly convert the DataFrame to DataSet
  case class Case(
                   dateRep: String,
                   month: Int,
                   year: Int,
                   cases: Int,
                   deaths: Int,
                   countriesAndTerritories: String,
                   continentExp: String
                 )

  def main(args: Array[String]): Unit = {
    // Use new SparkSession
    val spark = SparkSession
      .builder
      .appName("CovidReport")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Case case class
    // class to infer the schema.
    import spark.implicits._
    val cases = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/covid.csv")
      .as[Case]

    // Number of cases in Greece for month December
    println("Covid-19 cases in Greece for everyday of December")
    cases
      .select("dateRep", "countriesAndTerritories", "cases")
      .filter($"month" === 12)
      .filter($"year" === 2020)
      .filter($"countriesAndTerritories" === "Greece")
      .orderBy("dateRep")
      .show()

    // Total cases and deaths for every continent
    println("Covid-19 total cases and deaths for every continent")
    cases
      .groupBy("continentExp")
      .agg(sum("cases").alias("total_cases"),
        sum("deaths").alias("total_deaths"))
      .orderBy("continentExp")
      .show()

    // Average deaths and cases per country
    println("Average Covid-19 cases and deaths for every country in Europe")
    cases
      .filter($"continentExp" === "Europe")
      .groupBy("countriesAndTerritories")
      .agg(round(avg("cases"), 2).alias("average_cases"),
        round(avg("deaths"), 2).alias("average_deaths"))
      .orderBy("countriesAndTerritories")
      .show(cases.count.toInt)

    // Total cases per date in desc order for Europe
    println("10 days with the most Covid-19 cases in Europe")
    cases
      .filter($"continentExp" === "Europe")
      .groupBy("dateRep")
      .agg(sum("cases").alias("total_cases"))
      .orderBy(desc("total_cases"))
      .show(10)
  }

}

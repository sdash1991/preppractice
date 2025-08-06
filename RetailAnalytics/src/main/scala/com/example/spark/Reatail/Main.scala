package com.example.spark.Reatail

import com.example.spark.Reatail.ingestion.DataIngestion
import com.example.spark.Reatail.transformation.RetailTransformer
import com.example.spark.Reatail.writer.OutputWriter
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop-2.7.4")
    System.setProperty("hadoop.native.lib", "false")

    println("Building spark app")
    val spark = SparkSession.builder()
      .appName("Retail ETL Modular")
      .master("local[*]")
      .getOrCreate()
    println("spark app started.....")

    val basePath = "E:\\retalianalytics\\input"
    println("input path :" + s"$basePath")

    val sales = DataIngestion.readCSV(spark, s"$basePath\\sales.csv")
    val customers = DataIngestion.readCSV(spark, s"$basePath\\customers.csv")
    val countries = DataIngestion.readCSV(spark, s"$basePath\\countries.csv")
    sales.show(10)
    customers.show(10)
    countries.show(10)
    println("Read completed and created data frames")
    println("Transformation starting..")
    val enriched = RetailTransformer.enrichSales(sales, customers, countries)
    val agg = RetailTransformer.aggregateByCountry(enriched)
    println("Transformation completed..")

    println("Writer operation starting..")

    OutputWriter.writeToCSV(enriched,"E:\\retalianalytics\\output\\encriched_sales")
    OutputWriter.writeToCSV(agg,"E:\\retalianalytics\\output\\sales_summary_bycountry")

    println("Writer operation completed..")
    println("check output files in folder: /output")

    println("✅ ETL completed.")
    spark.stop()
  }
}

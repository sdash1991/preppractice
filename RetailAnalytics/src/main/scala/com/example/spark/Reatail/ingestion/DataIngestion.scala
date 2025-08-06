package com.example.spark.Reatail.ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIngestion {
  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
}

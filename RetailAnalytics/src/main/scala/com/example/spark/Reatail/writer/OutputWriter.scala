package com.example.spark.Reatail.writer

import org.apache.spark.sql.DataFrame

object OutputWriter {
  def writeToCSV(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }
}

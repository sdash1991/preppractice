package com.example.spark.Reatail.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RetailTransformer {

  def enrichSales(sales: DataFrame, customers: DataFrame, countries: DataFrame): DataFrame = {
    val salesWithTotal = sales.withColumn("total_amount", col("quantity") * col("unit_price"))
    val enriched = salesWithTotal
      .join(customers, "customer_id")
      .join(countries, "country_code")
    enriched
  }

  def aggregateByCountry(enrichedSales: DataFrame): DataFrame = {
    enrichedSales.groupBy("country_name")
      .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("revenue")
      )
      .orderBy(desc("revenue"))
  }
}

package com.vladaver.data_processing.service.impl

import com.vladaver.data_processing.service.PollutionDataService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

class PollutionDataServiceImpl(implicit sc: SQLContext) extends PollutionDataService {

  override def calculatePollutionStats(legendDF: DataFrame, measureDF: DataFrame): DataFrame = {

    val pollutionLegendDF = preparePollutionLegendDataframe(legendDF)
    val miDF = prepareMiDataFrame(measureDF)
    miDF.join(broadcast(pollutionLegendDF), "sensor_id")
  }

  private def preparePollutionLegendDataframe(legendDF: DataFrame): DataFrame = {
    legendDF
      .select(
        column("sensor_id"),
        column("sensor_street_name"),
        column("sensor_lat"),
        column("sensor_long"),
        column("sensor_type")
      )
  }

  private def prepareMiDataFrame(measureDF: DataFrame): DataFrame = {
    measureDF
      .withColumn("time_instant", to_timestamp(column("time_instant"), "yyyy/MM/dd HH:mm"))
      .filter(month(column("time_instant")).equalTo(12))
      .select(
        column("sensor_id"),
        column("time_instant"),
        column("measurement")
      )
      .groupBy(column("sensor_id"), month(column("time_instant")).as("month"))
      .agg(
        sum(column("measurement")).as("sum_measurement")
      )
      .drop(column("month"))
  }
}

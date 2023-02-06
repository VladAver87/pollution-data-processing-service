package com.vladaver.data_processing.service.impl

import com.vladaver.data_processing.schemas.Schemas.{activitiesSchema, pollutionLegendSchema, pollutionMISchema}
import com.vladaver.data_processing.service.PollutionDataService
import com.vladaver.data_processing.utils.ReadUtils.readDataset
import org.apache.spark.sql
import org.apache.spark.sql.functions.{broadcast, column, date_format, days, decode, encode, from_unixtime, lit, month, sum, to_timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

class PollutionDataServiceImpl(implicit sc: SQLContext) extends PollutionDataService {

  override def calculatePollutionStats(pathToLegend: String, pathToMeasureData: String): Unit = {

    val pollutionLegendDF = preparePollutionLegendDataframe(pathToLegend)
    val miDF = prepareMiDataFrame(pathToMeasureData)
    miDF.join(broadcast(pollutionLegendDF), "sensor_id")
  }

  private def preparePollutionLegendDataframe(pathToLegend: String): DataFrame = {
    readDataset(path = pathToLegend, schema = pollutionLegendSchema, sep = ",")
      .select(
        column("sensor_id"),
        column("sensor_street_name"),
        column("sensor_lat"),
        column("sensor_long"),
        column("sensor_type")
      )
  }

  private def prepareMiDataFrame(pathToMeasureData: String): DataFrame = {
    readDataset(path = pathToMeasureData, schema = pollutionMISchema, sep = ",")
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

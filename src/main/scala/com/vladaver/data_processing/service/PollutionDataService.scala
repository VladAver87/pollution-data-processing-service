package com.vladaver.data_processing.service

import org.apache.spark.sql.DataFrame

trait PollutionDataService {
  def calculatePollutionStats(pathToLegend: String, pathToMeasureData: String): DataFrame
}

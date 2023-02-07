package com.vladaver.data_processing.service

import org.apache.spark.sql.DataFrame

trait PollutionDataService {
  def calculatePollutionStats(legendDF: DataFrame, measureDF: DataFrame): DataFrame
}

package com.vladaver.data_processing.service

trait PollutionDataService {

  def calculatePollutionStats(pathToLegend: String, pathToMeasureData: String): Unit
}

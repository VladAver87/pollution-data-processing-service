package com.vladaver.data_processing.service

import org.apache.spark.sql.DataFrame

trait UserActivitiesService {

  def calculateActivitiesStats(activitiesData: DataFrame): DataFrame
}

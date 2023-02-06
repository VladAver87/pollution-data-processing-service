package com.vladaver.data_processing.service

import org.apache.spark.sql.DataFrame

trait GeoService {

  def transformGeoDataToDataframe(pathToDataset: String): DataFrame

}

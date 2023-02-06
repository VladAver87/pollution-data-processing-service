package com.vladaver.data_processing

import com.typesafe.config.ConfigFactory
import com.vladaver.data_processing.service.impl.{GeoServiceImpl, UserActivitiesServiceImpl}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{SQLContext, SparkSession}

object PollutionDataProcessingApp {

  def main(args: Array[String]): Unit = {
    val userActivitiesDataPath = ConfigFactory.load().getString("datasets.paths.usrActivitiesPath")
    val geoJsonPath = ConfigFactory.load().getString("datasets.paths.geoJsonPath")

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("PollutionDataProcessingApp")
      .getOrCreate()
    implicit val sc: SQLContext = sparkSession.sqlContext

//    val activitiesDf = new UserActivitiesServiceImpl().calculateActivitiesStats(pathToDataset = userActivitiesDataPath)
//    val geoDataDf = new GeoServiceImpl().transformGeoDataToDataframe(pathToDataset = geoJsonPath)
//
//    val activitiesWithSquareCoordinatesDf = activitiesDf
//      .join(broadcast(geoDataDf), "square_id")
//      .cache()


  }

}

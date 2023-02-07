package com.vladaver.data_processing

import com.typesafe.config.ConfigFactory
import com.vladaver.data_processing.schemas.Schemas.{activitiesSchema, pollutionLegendSchema, pollutionMISchema}
import com.vladaver.data_processing.service.impl.{GeoServiceImpl, PollutionDataServiceImpl, UserActivitiesServiceImpl}
import com.vladaver.data_processing.utils.Utils
import com.vladaver.data_processing.utils.Utils.readDataset
import org.apache.spark.sql.functions.{broadcast, column, udf}
import org.apache.spark.sql.{SQLContext, SparkSession}

object PollutionDataProcessingApp {

  def main(args: Array[String]): Unit = {
    val userActivitiesDataPath = ConfigFactory.load().getString("datasets.paths.usrActivitiesPath")
    val geoJsonPath = ConfigFactory.load().getString("datasets.paths.geoJsonPath")
    val pollutionLegendPath = ConfigFactory.load().getString("datasets.paths.pollutionLegend")
    val pollutionMIPath = ConfigFactory.load().getString("datasets.paths.pollutionMi")

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("PollutionDataProcessingApp")
      .getOrCreate()
    implicit val sc: SQLContext = sparkSession.sqlContext
    val isPointInSquareUDF = udf(Utils.isPointInSquare _)

    val activitiesDF = readDataset(path = userActivitiesDataPath, schema = activitiesSchema)

    val activitiesStatsDf = new UserActivitiesServiceImpl().calculateActivitiesStats(activitiesDF)
    val geoDataDf = new GeoServiceImpl().transformGeoDataToDataframe(pathToDataset = geoJsonPath)

    val activitiesWithSquareCoordinatesDf = activitiesStatsDf
      .join(broadcast(geoDataDf), "square_id")

    val legendDF = readDataset(path = pollutionLegendPath, schema = pollutionLegendSchema, sep = ",")
    val measureDF = readDataset(path = pollutionMIPath, schema = pollutionMISchema, sep = ",")
    val pollutionSensorsDF = new PollutionDataServiceImpl().calculatePollutionStats(legendDF, measureDF)

    activitiesWithSquareCoordinatesDf
      .join(broadcast(pollutionSensorsDF),
        isPointInSquareUDF(column("coordinates"), column("sensor_lat"), column("sensor_long"))
      )
      .show(truncate = false)

  }

}

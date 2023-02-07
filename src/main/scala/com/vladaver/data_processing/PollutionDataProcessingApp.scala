package com.vladaver.data_processing

import com.typesafe.config.ConfigFactory
import com.vladaver.data_processing.liquibase.SchemaMigration
import com.vladaver.data_processing.schemas.Schemas.{activitiesSchema, pollutionLegendSchema, pollutionMISchema}
import com.vladaver.data_processing.service.impl.{GeoServiceImpl, PollutionDataServiceImpl, UserActivitiesServiceImpl}
import com.vladaver.data_processing.utils.Utils
import com.vladaver.data_processing.utils.Utils.{loadConfigs, readDataset}
import org.apache.spark.sql.functions.{array_join, broadcast, col, collect_list, column, concat, concat_ws, count, first, size, sum, udf}
import org.apache.spark.sql.{SQLContext, SparkSession}

object PollutionDataProcessingApp {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("PollutionDataProcessingApp")
      .getOrCreate()
    implicit val sc: SQLContext = sparkSession.sqlContext
    val isPointInSquareUDF = udf(Utils.isPointInSquare _)
    val config = loadConfigs()
    //new SchemaMigration(config).run()

    val activitiesDF = readDataset(path = config.usrActivitiesPath, schema = activitiesSchema)

    val activitiesStatsDf = new UserActivitiesServiceImpl().calculateActivitiesStats(activitiesDF)
    val geoDataDf = new GeoServiceImpl().transformGeoDataToDataframe(pathToDataset = config.geoJsonPath)

    val activitiesWithSquareCoordinatesDf = activitiesStatsDf
      .join(broadcast(geoDataDf), "square_id")

    val legendDF = readDataset(path = config.pollutionLegendPath, schema = pollutionLegendSchema, sep = ",")
    val measureDF = readDataset(path = config.pollutionMiPath, schema = pollutionMISchema, sep = ",")

    val pollutionSensorsDF = new PollutionDataServiceImpl().calculatePollutionStats(legendDF, measureDF)

    val resultDF = activitiesWithSquareCoordinatesDf
      .join(broadcast(pollutionSensorsDF),
        isPointInSquareUDF(column("coordinates"), column("sensor_lat"), column("sensor_long"))
      )
      .drop("coordinates", "sensor_lat", "sensor_long", "sensor_street_name", "sensor_id")
      .show(truncate = false, numRows = 1000)

    /*
    В результате выполнения данной Задачи необходимо составить отчет со следующими данными:
    - Топ 5 «загрязненных» рабочих и спальных зон.
    - Топ 5 «чистых» рабочих и спальных зон.
    - список не определенных по классу или по уровню загрязнения зон
    В отчете сохранить данные о концентрации веществ, количестве датчиков в зоне и пороговых значениях концентрации и активности.
     */
    /*
    - Топ 5 «загрязненных» рабочих
     */
//    resultDF
//      .select(
//        column("square_id"),
//        column("sum_measurement"),
//        column("sensor_type"),
//      )
//      .where(column("is_work_square").equalTo(true))
//      .groupBy(column("square_id"))
//      .agg(
//        sum(column("sum_measurement")).as("total_concentration"),
//        count(column("sensor_type")).as("sensors_count"),
//        concat_ws(",", collect_list(column("sensor_type"))).as("sensors_type")
//      )
//      .orderBy(column("total_concentration").desc)
//      .drop("is_work_square")
//      .show(truncate = false)
//
//    /*
//    - Топ 5 «загрязненных» спальных
//    */
//    resultDF
//      .select(
//        column("square_id"),
//        column("sum_measurement"),
//        column("sensor_type"),
//      )
//      .where(column("is_work_square").equalTo(false))
//      .groupBy(column("square_id"))
//      .agg(
//        sum(column("sum_measurement")).as("total_concentration"),
//        count(column("sensor_type")).as("sensors_count"),
//        concat_ws(",", collect_list(column("sensor_type"))).as("sensors_type")
//      )
//      .orderBy(column("total_concentration").desc)
//      .drop("is_work_square")
//      .show(truncate = false)
  }

}

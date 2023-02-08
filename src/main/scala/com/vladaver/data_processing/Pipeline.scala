package com.vladaver.data_processing

import com.vladaver.data_processing.schemas.Schemas.{activitiesSchema, pollutionLegendSchema, pollutionMISchema}
import com.vladaver.data_processing.service.impl.{GeoServiceImpl, PollutionDataServiceImpl, UserActivitiesServiceImpl}
import com.vladaver.data_processing.utils.Utils
import com.vladaver.data_processing.utils.Utils.{AppConfig, readDataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import java.util.Properties

class Pipeline(config: AppConfig)(implicit sc: SQLContext) {

  def calculateAggregatedData(): DataFrame = {

    val isPointInSquareUDF = udf(Utils.isPointInSquare _)

    val activitiesDF = readDataset(path = config.usrActivitiesPath, schema = activitiesSchema)
    val legendDF = readDataset(path = config.pollutionLegendPath, schema = pollutionLegendSchema, sep = ",")
    val measureDF = readDataset(path = config.pollutionMiPath, schema = pollutionMISchema, sep = ",")

    val activitiesStatsDf = new UserActivitiesServiceImpl().calculateActivitiesStats(activitiesDF)
    val geoDataDf = new GeoServiceImpl().transformGeoDataToDataframe(pathToDataset = config.geoJsonPath)
    val pollutionSensorsDF = new PollutionDataServiceImpl().calculatePollutionStats(legendDF, measureDF)

    activitiesStatsDf
      .join(broadcast(geoDataDf), "square_id")
      .join(broadcast(pollutionSensorsDF),
        isPointInSquareUDF(column("coordinates"), column("sensor_lat"), column("sensor_long")),
        joinType = "left"
      )
      .drop("sensor_lat", "sensor_long", "sensor_street_name", "sensor_id")
      .cache()
  }
}

object Pipeline {
  implicit class ExtendedDataFrame(val data: DataFrame) extends AnyVal {
    def generateResultReports(config: AppConfig): Unit = {
      /*
      - Топ 5 «загрязненных» рабочих
       */
      data
        .reportsTemplate(isWorkSquare = true)
        .orderBy(column("total_concentration").desc)
        .limit(5)
        .storeToPostgres(config, "Top5pollutedWorkingSquares")

      /*
      - Топ 5 «загрязненных» спальных
      */
      data
        .reportsTemplate(isWorkSquare = false)
        .orderBy(column("total_concentration").desc)
        .limit(5)
        .storeToPostgres(config, "Top5pollutedNonWorkingSquares")

      /*
      - Топ 5 «чистых» спальных
      */
      data
        .reportsTemplate(isWorkSquare = false)
        .orderBy(column("total_concentration"))
        .limit(5)
        .storeToPostgres(config, "Top5cleanNonWorkingSquares")
      /*
      - Топ 5 «чистых» рабочих
      */
      data
        .reportsTemplate(isWorkSquare = true)
        .orderBy(column("total_concentration"))
        .limit(5)
        .storeToPostgres(config, "Top5cleanWorkingSquares")

      /*
      - Список «неопределенных»
      */
      data
        .select(
          column("square_id"),
          column("coordinates")
        )
        .where(column("sum_measurement").isNull)
        .storeToPostgres(config, "UndefinedSquares")
    }

    def reportsTemplate(isWorkSquare: Boolean): DataFrame = {
      data
        .select(
          column("square_id"),
          column("coordinates"),
          column("sum_measurement"),
          column("sensor_type"),
        )
        .where(
          column("is_work_square").equalTo(isWorkSquare)
            .and(column("sum_measurement").isNotNull))
        .groupBy(column("square_id"), column("coordinates"))
        .agg(
          sum(column("sum_measurement")).as("total_concentration"),
          count(column("sensor_type")).as("sensors_count"),
          concat_ws(",", collect_list(column("sensor_type"))).as("sensors_type")
        )
        .drop("is_work_square")
    }
  }

  implicit class ExtendedDataSet(val data: Dataset[Row]) extends AnyVal {
    def storeToPostgres(config: AppConfig, table: String): Unit = {
      val properties = new Properties()
      properties.setProperty("user", config.pgUsername)
      properties.setProperty("password", config.pgPassword)
      properties.put("driver", "org.postgresql.Driver")
      data.write.mode(SaveMode.Overwrite)
        .jdbc(config.pgUrl, table, properties)
    }
  }

}

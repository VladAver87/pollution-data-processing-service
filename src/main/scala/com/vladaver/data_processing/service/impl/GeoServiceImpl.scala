package com.vladaver.data_processing.service.impl

import com.vladaver.data_processing.service.GeoService
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset.format
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source.fromFile

case class Properties(cellId: Long)
case class Geometry(coordinates: List[List[List[Double]]])
case class Features(geometry: Geometry, properties: Properties)
case class GeoData(features: List[Features])

class GeoServiceImpl(implicit sc: SQLContext) extends GeoService {

  override def transformGeoDataToDataframe(pathToDataset: String): DataFrame = {
    val mappedGeoData = parseJson(path = pathToDataset)

    import sc.implicits._
    mappedGeoData.toDF("square_id", "coordinates")
  }

  private def parseJson(path: String): List[(Long, List[List[Double]])] = {
    val data = loadDataFromSource(path = path)
    val geoJson = parse(data)
    val geoData = geoJson.extract[GeoData]
    geoData.features.map(item => item.properties.cellId ->
      item.geometry.coordinates.flatten
    )
  }

  private def loadDataFromSource(path: String): String = {
    val source = fromFile(path)
    try source.mkString finally source.close()
  }

}

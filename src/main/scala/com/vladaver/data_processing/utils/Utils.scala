package com.vladaver.data_processing.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType


object Utils {
  case class Point(x: Double, y: Double)

  case class AppConfig(
                           usrActivitiesPath: String,
                           geoJsonPath: String,
                           pollutionLegendPath: String,
                           pollutionMiPath: String,
                           pgUrl: String,
                           pgUsername: String,
                           pgPassword: String
                         )

  def loadConfigs(): AppConfig = {
    val userActivitiesDataPath = ConfigFactory.load().getString("datasets.usrActivitiesPath")
    val geoJsonPath = ConfigFactory.load().getString("datasets.geoJsonPath")
    val pollutionLegendPath = ConfigFactory.load().getString("datasets.pollutionLegendPath")
    val pollutionMIPath = ConfigFactory.load().getString("datasets.pollutionMiPath")
    val url = ConfigFactory.load().getString("postgres.pgUrl")
    val user = ConfigFactory.load().getString("postgres.pgUsername")
    val pass = ConfigFactory.load().getString("postgres.pgPassword")
    AppConfig(userActivitiesDataPath, geoJsonPath, pollutionLegendPath, pollutionMIPath, url, user, pass)
  }

  def readDataset(path: String, schema: StructType, sep: String = "\t")(implicit sc: SQLContext): DataFrame = {
    sc.read
      .options(Map("sep" -> sep, "header" -> "false"))
      .schema(schema)
      .csv(path = path)
  }

  def isPointInSquare(square: Array[String], pointLat: String, pointLong: String): Boolean = {
      val sqPoint1 = Point(square(0).toDouble - 0.0003, square(1).toDouble + 0.0002)
      val sqPoint2 = Point(square(2).toDouble + 0.0003, square(3).toDouble + 0.0002)
      val sqPoint3 = Point(square(4).toDouble + 0.0003, square(5).toDouble - 0.0002)
      val sqPoint4 = Point(square(6).toDouble - 0.0003, square(7).toDouble - 0.0002)

      val givenPoint = Point(pointLong.toDouble, pointLat.toDouble)

      checkIsPointInSquare(sqPoint1, sqPoint2, sqPoint3, sqPoint4, givenPoint)
  }

  private def calculateTriangleArea(point1: Point, point2: Point, point3: Point): Double = {
    Math.abs(
      (point1.x * (point2.y - point3.y) + point2.x * (point3.y - point1.y) + point3.x * (point1.y - point2.y)) / 2.0)
  }

  private def checkIsPointInSquare(point1: Point, point2: Point, point3: Point, point4: Point,
                                   givenPoint: Point): Boolean = {
    val rectSquare = Math.abs((point1.x*(point2.y-point3.y) + point2.x*(point3.y-point1.y) + point3.x*(point1.y-point2.y))/2) +
      Math.abs((point1.x*(point4.y-point3.y) + point4.x*(point3.y-point1.y) + point3.x*(point1.y-point4.y))/2)

    val squareFirstTriangle = calculateTriangleArea(givenPoint, point1, point2)
    val squareSecondTriangle = calculateTriangleArea(givenPoint, point2, point3)
    val squareThirdTriangle = calculateTriangleArea(givenPoint, point3, point4)
    val squareFourthTriangle = calculateTriangleArea(givenPoint, point1, point4)
    val sum = squareFirstTriangle + squareSecondTriangle + squareThirdTriangle + squareFourthTriangle
    rectSquare.toFloat == sum.toFloat
  }
}

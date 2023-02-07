package com.vladaver.data_processing.utils

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType


object Utils {
  case class Point(x: Float, y: Float)

  def readDataset(path: String, schema: StructType, sep: String = "\t")(implicit sc: SQLContext): DataFrame = {
    sc.read
      .options(Map("sep" -> sep, "header" -> "false"))
      .schema(schema)
      .csv(path = path)
  }

  def isPointInSquare(square: Array[String], pointLat: String, pointLong: String): Boolean = {
      val sqPoint1 = Point(square(0).toFloat, square(1).toFloat)
      val sqPoint2 = Point(square(2).toFloat, square(3).toFloat)
      val sqPoint3 = Point(square(4).toFloat, square(5).toFloat)
      val sqPoint4 = Point(square(6).toFloat, square(7).toFloat)

      val givenPoint = Point(pointLat.toFloat, pointLong.toFloat)

      checkIsPointInSquare(sqPoint1, sqPoint2, sqPoint3, sqPoint4, givenPoint)
  }

  private def calculateTriangleArea(point1: Point, point2: Point, point3: Point): Float = {
    Math.abs(
      (point1.x * (point2.y - point3.y) + point2.x * (point3.y - point1.y) + point3.x * (point1.y - point2.y)) / 2.0).toFloat
  }

  private def checkIsPointInSquare(point1: Point, point2: Point, point3: Point, point4: Point,
                                   givenPoint: Point): Boolean = {
    val rectSquare = Math.abs((point1.x*(point2.y-point3.y) + point2.x*(point3.y-point1.y) + point3.x*(point1.y-point2.y))/2) +
      Math.abs((point1.x*(point4.y-point3.y) + point4.x*(point3.y-point1.y) + point3.x*(point1.y-point4.y))/2)

    val squareFirstTriangle = calculateTriangleArea(givenPoint, point1, point2)
    val squareSecondTriangle = calculateTriangleArea(givenPoint, point2, point3)
    val squareThirdTriangle = calculateTriangleArea(givenPoint, point3, point4)
    val squareFourthTriangle = calculateTriangleArea(givenPoint, point1, point4)

    rectSquare == (squareFirstTriangle + squareSecondTriangle + squareThirdTriangle + squareFourthTriangle)
  }
}

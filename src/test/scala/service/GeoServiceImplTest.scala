package service

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.vladaver.data_processing.service.impl.GeoServiceImpl
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GeoServiceImplTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Json" should "be parsed correctly" in {
    val sourceFile = getClass.getResource("/geoJsonTest.json").getPath
    val expectedDF = List((1L, List(9.011491, 45.358803, 9.014491, 45.358803, 9.014491, 45.356686, 9.011491, 45.356686)
        .map(_.toFloat)))
        .toDF("square_id", "coordinates")
    val actualDF = new GeoServiceImpl().transformGeoDataToDataframe(sourceFile)

    assertDataFrameEquals(expectedDF, actualDF)
  }

}

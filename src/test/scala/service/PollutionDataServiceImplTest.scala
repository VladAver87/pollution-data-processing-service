package service

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.vladaver.data_processing.service.impl.PollutionDataServiceImpl
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PollutionDataServiceImplTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Result dataframe" should "be correct" in {
    val legendDF = Seq((5841, "Milano - P.zza  Zavattari", 45.47609, 9.143509, "Carbon Monoxide", "mg/m3", "YYYY/MM/DD HH24:MI"))
      .toDF("sensor_id", "sensor_street_name", "sensor_lat", "sensor_long", "sensor_type", "uom", "time_instant_format")
    val measureDF = Seq((5841, "2013/12/25 08:00", 1.toLong))
      .toDF("sensor_id", "time_instant", "measurement")
    val expectedDF = Seq((5841, 1.0.toLong, "Milano - P.zza  Zavattari", 45.47609, 9.143509, "Carbon Monoxide"))
      .toDF("sensor_id", "sum_measurement", "sensor_street_name", "sensor_lat", "sensor_long", "sensor_type")

    val actualDF = new PollutionDataServiceImpl().calculatePollutionStats(legendDF, measureDF)
    assertDataFrameDataEquals(expectedDF, actualDF)
  }

}

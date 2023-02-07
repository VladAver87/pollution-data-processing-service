import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.vladaver.data_processing.utils.Utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{broadcast, column, udf}
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinByCoordinatesTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Dataframes" should "be joined correctly by coordinates" in {
    val isPointInSquareUDF = udf(Utils.isPointInSquare _)

    val activitiesWithSquareCoordinatesDf = List((1L, List(9.011491, 45.358803, 9.014491, 45.358803, 9.014491,
      45.356686, 9.011491, 45.356686)
      .map(_.toFloat)))
      .toDF("square_id", "coordinates")

    val pollutionSensorsDF = Seq((5841, 1.0.toLong, "Milano - P.zza  Zavattari", 45.357000, 9.012000, "Carbon Monoxide"))
      .toDF("sensor_id", "sum_measurement", "sensor_street_name", "sensor_lat", "sensor_long", "sensor_type")

    val expectedDF = Seq((1, 5841, 1.0.toLong, "Milano - P.zza  Zavattari", "Carbon Monoxide"))
      .toDF("square_id", "sensor_id", "sum_measurement", "sensor_street_name", "sensor_type")


    val actualDF = activitiesWithSquareCoordinatesDf
      .join(broadcast(pollutionSensorsDF),
        isPointInSquareUDF(column("coordinates"), column("sensor_lat"), column("sensor_long"))
      )
      .drop("coordinates", "sensor_lat", "sensor_long")

    assertDataFrameDataEquals(expectedDF, actualDF)
  }

  "Dataframes" should "be not joined" in {
    val isPointInSquareUDF = udf(Utils.isPointInSquare _)
    val emptyDFschema = new StructType()
      .add("square_id", LongType)
      .add("sensor_id", LongType)
      .add("sum_measurement", LongType)
      .add("sensor_street_name", StringType)
      .add("sensor_type", StringType)


    val activitiesWithSquareCoordinatesDf = List((1L, List(9.011491, 45.358803, 9.014491, 45.358803, 9.014491,
      45.356686, 9.011491, 45.356686)
      .map(_.toFloat)))
      .toDF("square_id", "coordinates")

    val pollutionSensorsDF = Seq((5841, 1.0.toLong, "Milano - P.zza  Zavattari", 45.359000, 9.012000, "Carbon Monoxide"))
      .toDF("sensor_id", "sum_measurement", "sensor_street_name", "sensor_lat", "sensor_long", "sensor_type")

    val expectedDF = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], emptyDFschema)

    val actualDF = activitiesWithSquareCoordinatesDf
      .join(broadcast(pollutionSensorsDF),
        isPointInSquareUDF(column("coordinates"), column("sensor_lat"), column("sensor_long"))
      )
      .drop("coordinates", "sensor_lat", "sensor_long")


    assertDataFrameDataEquals(expectedDF, actualDF)
  }

}

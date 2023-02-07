package utils

import com.vladaver.data_processing.utils.Utils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsTest extends AnyFlatSpec with Matchers {

  "Point" should "be in square" in {
    val squarePoints = Array("0", "0", "10", "0", "10", "10", "0", "10")
    val givenPointLat = "1"
    val givenPointLong = "2"

    val result = Utils.isPointInSquare(squarePoints, givenPointLat, givenPointLong)

    result shouldBe true
  }

  "Point" should "not be in square" in {
    val squarePoints = Array("0", "0", "10", "0", "10", "10", "0", "10")
    val givenPointLat = "12"
    val givenPointLong = "11"

    val result = Utils.isPointInSquare(squarePoints, givenPointLat, givenPointLong)

    result shouldBe false
  }
}

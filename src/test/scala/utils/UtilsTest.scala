package utils

import com.vladaver.data_processing.utils.Utils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsTest extends AnyFlatSpec with Matchers {
  /*
  remember that total square in isPointInSquare method is increased by 10%
   */

  "Point" should "be in square" in {
    val squarePoints = Array("0", "0", "10", "0", "10", "10", "0", "10")
    val givenPointLat = "1"
    val givenPointLong = "2"

    val result = Utils.isPointInSquare(squarePoints, givenPointLat, givenPointLong)

    result shouldBe true
  }

  "Point" should "be in square too" in {
    val squarePoints = Array("9.011491", "45.358803", "9.014491", "45.358803", "9.014491", "45.356686", "9.011491", "45.356686")
    val givenPointLat = "45.358803"
    val givenPointLong = "9.011491"

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

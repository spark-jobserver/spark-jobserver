package spark.jobserver.python

import java.util

import com.typesafe.config.ConfigFactory
import ooyala.common.akka.web.JsonUtils.AnyJsonFormat
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.python.PythonJob.ConversionException
import spray.json._

import scala.util.Failure

class PythonJobSpec extends FunSpec with Matchers {

  describe("PythonJob data conversion") {

    it("should convert HashMaps to a type which can be serialized by spray-json") {
      val hashMap = new java.util.HashMap[Any, Any]()
      hashMap.put("a", 1)
      hashMap.put("b", 2)
      hashMap.put("c", 3)
      PythonJob.convertRawResult(hashMap).get.toJson should be (
        JsObject("a" -> JsNumber(1), "b" -> JsNumber(2), "c" -> JsNumber(3)))
    }

    it("should convert ArrayLists to a type which can be serialized by spray-json") {
      val arrayList = new java.util.ArrayList[Any]()
      arrayList.add(1)
      arrayList.add("a")
      arrayList.add(2.0)
      PythonJob.convertRawResult(arrayList).get.toJson should be (
        JsArray(JsNumber(1), JsString("a"), JsNumber(2.0))
      )
    }

    it("should leave primitive types unchanged") {
      PythonJob.convertRawResult(1).get should be (1)
      PythonJob.convertRawResult("hello world").get should be ("hello world")
      PythonJob.convertRawResult(2L).get should be (2L)
      PythonJob.convertRawResult(3.0d).get should be (3.0d)
      PythonJob.convertRawResult(4.0f).get should be (4.0f)
    }

    it("should convert nested ArrayLists") {
      val input = new java.util.ArrayList[java.util.ArrayList[Int]]()
      val subList1 = new util.ArrayList[Int]()
      subList1.add(1)
      subList1.add(2)
      subList1.add(3)
      val subList2 = new util.ArrayList[Int]()
      subList2.add(4)
      subList2.add(5)
      subList2.add(6)
      val subList3 = new util.ArrayList[Int]()
      subList3.add(7)
      subList3.add(8)
      input.add(subList1)
      input.add(subList2)
      input.add(subList3)
      PythonJob.convertRawResult(input).get should be (Seq(Seq(1,2,3), Seq(4,5,6), Seq(7,8)))
    }

    it("should convert nested Maps") {
      val input = new java.util.HashMap[String, java.util.HashMap[String, String]]()
      val subMap1 = new java.util.HashMap[String, String]()
      subMap1.put("A", "1")
      subMap1.put("B", "2")
      subMap1.put("C", "3")
      val subMap2 = new java.util.HashMap[String, String]()
      subMap2.put("D", "4")
      subMap2.put("E", "5")
      subMap2.put("F", "6")
      val subMap3 = new java.util.HashMap[String, String]()
      subMap3.put("G", "7")
      subMap3.put("H", "8")
      input.put("X", subMap1)
      input.put("Y", subMap2)
      input.put("Z", subMap3)
      PythonJob.convertRawResult(input).get should be (
        Map(
          "X" -> Map("A" -> "1", "B" -> "2", "C" -> "3"),
          "Y" -> Map("D" -> "4", "E" -> "5", "F" -> "6"),
          "Z" -> Map("G" -> "7", "H" -> "8")
        )
      )
    }

    it("should convert nested Maps and ArrayLists in combination") {
      val input = new java.util.HashMap[String, java.util.ArrayList[Int]]()
      val subList1 = new util.ArrayList[Int]()
      subList1.add(1)
      subList1.add(2)
      subList1.add(3)
      val subList2 = new util.ArrayList[Int]()
      subList2.add(4)
      subList2.add(5)
      subList2.add(6)
      val subList3 = new util.ArrayList[Int]()
      subList3.add(7)
      subList3.add(8)
      input.put("X", subList1)
      input.put("Y", subList2)
      input.put("Z", subList3)
      PythonJob.convertRawResult(input).get should be (
        Map(
          "X" -> Seq(1,2,3),
          "Y" -> Seq(4,5,6),
          "Z" -> Seq(7,8)
        )
      )
    }

    it("should fail if one element of an list is not convertible") {
      val input = new java.util.ArrayList[Any]()
      input.add(1)
      input.add(2)
      val nonConvertible = ConfigFactory.empty()
      input.add(nonConvertible)
      PythonJob.convertRawResult(input) should matchPattern {
        case Failure(ConversionException(`nonConvertible`, _)) =>
      }
    }

    it("should fail if one value of a map is not convertible") {
      val input = new java.util.HashMap[String, Any]()
      input.put("A", 1)
      input.put("B", 2)
      val nonConvertible = ConfigFactory.empty()
      input.put("C", nonConvertible)
      PythonJob.convertRawResult(input) should matchPattern {
        case Failure(ConversionException(`nonConvertible`, _)) =>
      }
    }
  }
}

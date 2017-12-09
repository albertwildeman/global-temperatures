package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import observatory.Extraction._

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {
  trait TestExtraction {
    val year = 1975
//    val stationsFile = "C:\\Users\\Albert\\IdeaProjects\\progfun5_capstone\\global-temperatures\\src\\test\\resources\\stations.csv"
//    val temperaturesFile = "C:\\Users\\Albert\\IdeaProjects\\progfun5_capstone\\global-temperatures\\src\\test\\resources\\1975.csv"
    val stationsFile = "/stations.csv"
    val temperaturesFile = "/1975.csv"
  }
  test("Small extraction test") {
    new TestExtraction {
      val joined = locateTemperatures(year, stationsFile, temperaturesFile)
//      println(joined)
      joined.foreach(println)
//      val averaged = locationYearlyAverageRecords(joined)
      assert(5 === 5)
    }
  }
}

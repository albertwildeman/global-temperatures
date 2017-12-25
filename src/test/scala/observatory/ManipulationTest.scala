package observatory

import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}
import Manipulation._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

trait ManipulationTest extends FunSuite with Checkers {

}
@RunWith(classOf[JUnitRunner])
class ManipulationTestSeparate extends FunSuite {

  trait TestGrid {
    val year = 1975
    val stationsFile = "/stations.csv"
    val temperaturesFile = "/1975.csv"

    val averagedTemps: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(
      locateTemperatures(year, stationsFile, temperaturesFile)
    )
  }
  test("Small grid test") {
    new TestGrid {
      val gridFunc = makeGrid(averagedTemps)
      println(gridFunc(GridLocation(0,0)))
      assert(5 === 5)
    }
  }


}

//class ExtractionTest extends FunSuite {
//  trait TestExtraction {
//    val year = 1975
//    //    val stationsFile = "C:\\Users\\Albert\\IdeaProjects\\progfun5_capstone\\global-temperatures\\src\\test\\resources\\stations.csv"
//    //    val temperaturesFile = "C:\\Users\\Albert\\IdeaProjects\\progfun5_capstone\\global-temperatures\\src\\test\\resources\\1975.csv"
//    val stationsFile = "/stations.csv"
//    val temperaturesFile = "/1975.csv"
//  }
//  test("Small extraction test") {
//    new TestExtraction {
//      val joined = locateTemperatures(year, stationsFile, temperaturesFile)
//      //      println(joined)
//      //      joined.foreach(println)
//      val averaged = locationYearlyAverageRecords(joined)
//      //      val averaged = locationYearlyAverageRecords(joined)
//      averaged.foreach(println)
//      assert(5 === 5)
//    }
//  }
//}

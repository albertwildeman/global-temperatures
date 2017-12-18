package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.concurrent.TrieMap
import Visualization._

trait InteractionTest extends FunSuite with Checkers {
  test("visualize grader 2") {

    val locations = List(
      (new Location(45.0, -90.0), 20.0), (new Location(45.0, 90.0), 0.0), (new Location(0.0, 0.0), 10.0), (new Location(-45.0, -90.0), 0.0), (new Location(-45.0, 90.0), 20.0))
    val colorMap = List(
      (0.0, Color(255, 0, 0)), (10.0, Color(0, 255, 0))
      ,(20.0 , Color(0  , 0  , 255))
    )
    val pt: Location = Location(-27.059125784374057,-178.59375)
    val temperature = predictTemperature(locations, pt)

    val color = interpolateColor(colorMap, temperature)
    println((color, temperature))

    assert(color.red>color.blue, "interpolateColor")
  }
}

package observatory

import com.sksamuel.scrimage.Image
import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}
import observatory.Visualization.visualize
object Main extends App {

  val year = 1975
  val stationsFile = "/stations.csv"
  val temperaturesFile = "/1975.csv"

  val averagedTemps: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(
    locateTemperatures(year, stationsFile, temperaturesFile)
  )

  val colors: Iterable[(Temperature, Color)] = Array(
    (60.0, Color(255,255,255)),
    (32.0, Color(255,0,0)),
    (12.0, Color(255,255,0)),
    (0.0, Color(0,255,255)),
    (-15.0, Color(0,0,255)),
    (-27.0, Color(255,0,255)),
    (-50.0, Color(33,0,107)),
    (-60.0, Color(0,0,0))
  )

  val yearlyData: Iterable[(Year, Iterable[(Location, Temperature)])] =
    List((1975, averagedTemps))

  def generateImage(year: Year,
                    tile: Tile,
                    data: Iterable[(Location, Temperature)]): Unit = {

    val temperatures: Iterable[(Location, Temperature)] = data
//    val img = visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)])

    val img = Interaction.tile(temperatures, colors, tile)

    val outFile =
      "target\\temperatures\\" +
      year.toString() + "\\" +
      tile.zoom.toString() + "\\" +
      tile.x.toString() + "-" +
      tile.y.toString() + ".png"
    img.output(new java.io.File(outFile))
  }
  println("yay2.")
  Interaction.generateTiles[Iterable[(Location, Temperature)]](
    yearlyData,
    generateImage
  )
  println("yay3.")

}

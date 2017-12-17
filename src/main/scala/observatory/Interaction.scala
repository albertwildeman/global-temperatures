package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._
import Visualization.visualizeAny

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {



  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    Location(
      toDegrees(atan(sinh(Pi * (1.0 - 2.0 * tile.y.toDouble / (1<<tile.zoom))))),
      tile.x.toDouble / (1<<tile.zoom) * 360.0 - 180.0
    )
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val topLeft = tileLocation(tile)
    val bottomRight = tileLocation(Tile(tile.x+1, tile.y+1, tile.zoom))
//    val tileTemperatures = temperatures.filter{case (loc, temp) =>
//      loc.lat>=topLeft.lat &&
//      loc.lon>=topLeft.lon &&
//      loc.lat<bottomRight.lat &&
//      loc.lon<bottomRight.lon
//    }

    val width = 256
    val height= 256


    def locate(w: Int, h: Int, topLt: Location, botRt: Location)(x: Int, y: Int): Location = {
      val dLongitude: Double = (botRt.lon - topLt.lon) / width
      val dLatitude:  Double = (botRt.lat - topLt.lat) / height

      val longitude = topLt.lon + x*dLongitude
      val latitude  = topLt.lat - y*dLatitude

      Location(latitude, longitude)
    }
    visualizeAny(
      temperatures,
      colors,
      locate(width, height, topLeft, bottomRight),
      width,
      height)
  }


  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {

    for { yearData <- yearlyData
          zoom <- 0 to 3
          xTile <- 0 until (1<<zoom)
          yTile <- 0 until (1<<zoom)
    } generateImage(yearData._1, Tile(xTile, yTile, zoom), yearData._2)
  }

}

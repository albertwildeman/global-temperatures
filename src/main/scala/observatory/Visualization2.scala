package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import Visualization.interpolateColor
import Interaction.tileLocation
import Math.{floor, ceil}
/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    d00*(1-point.x)*(1-point.y) + d10*point.x*(1-point.y) +
      d01*(1-point.x)*point.y + d11*point.x*point.y
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {

    val width = 256
    val height = 256

    def tempAtPixel(x: Int, y: Int): Temperature = {

      val location = tileLocation( Tile(
          tile.x*width + x,
          tile.y*height + y,
          tile.zoom+8))

      def periodicBoundary(in: Int, low: Int, high: Int): Int = {
        val size: Int = 1+high-low
        if ((in >= low) && (in <= high)) in
        else if (in < low) in + size
        else in - size
      }

      val lowerLat = periodicBoundary(floor(location.lat).toInt, -89, 90)
      val lowerLon = periodicBoundary(floor(location.lon).toInt, -180, 179)
      val upperLat = periodicBoundary(ceil(location.lat).toInt, -89, 90)
      val upperLon = periodicBoundary(ceil(location.lon).toInt, -180, 179)

      val d00: Temperature = grid(GridLocation(lowerLon, lowerLat))
      val d01: Temperature = grid(GridLocation(upperLon, lowerLat))
      val d10: Temperature = grid(GridLocation(lowerLon, upperLat))
      val d11: Temperature = grid(GridLocation(upperLon, upperLat))

      val cellPoint: CellPoint = CellPoint(location.lat - floor(location.lat),
        location.lon - floor(location.lon))

      bilinearInterpolation(cellPoint, d00, d01, d10, d11)
    }

    val colorArray = new Array[Color](width*height)
    for (x <- 0 until width; y <- 0 until height) {
      colorArray(x + width*y) =
        interpolateColor(colors, tempAtPixel(x,y))
    }

    val pixelArray: Array[Pixel] =
      colorArray.map(c => Pixel(c.red, c.green, c.blue, 127))
    val img = Image(width, height, pixelArray)
    img
  }

}

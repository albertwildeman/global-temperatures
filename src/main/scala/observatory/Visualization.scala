package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {

    // Function to approximate the distance (in km) between two locations
    def distance(loc1: Location, loc2: Location): Double = {

      val ds ={
        val radLonDiff = Math.toRadians(loc2.lon-loc1.lon).abs

        // equal points
        if (loc1.equals(loc2)) 0.0
        // antipodes
        if ((loc1.lat == -1*loc2.lat) && (loc2.lon-loc1.lon).abs==180) Math.PI
        // otherwise
        else{
          val radLat1 = Math.toRadians(loc1.lat)
          val radLat2 = Math.toRadians(loc2.lat)
          Math.acos(  Math.sin(radLat1) * Math.sin(radLat2)
                    + Math.cos(radLat1) * Math.cos(radLat2) * Math.cos(radLonDiff))
        }
      }

      val radius_earth = 6371
      radius_earth * ds
    }

    val distanceAndTemp: Iterable[(Double, Temperature)] =
      temperatures.map{case (loc2, temp) => (distance(location, loc2), temp)}

    val veryClose = distanceAndTemp.filter(_._1 <= 1)

    // If any point is very close, assign the temperature of that point
    if (veryClose.size > 0) veryClose.head._2
    else {
      val weightAndTemp = distanceAndTemp
        .map{case (dist, temp) => (1.0/Math.pow(dist, 4), temp)}

      val sumWeights: Double = weightAndTemp.map(_._1).reduce(_+_)

      val numerator: Double = weightAndTemp
        .map{case (weight, temp) => weight * temp}
        .reduce(_+_)

      numerator / sumWeights
    }

  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {

    val lessOrEqual = points.filter(_._1 <= value)
    val greater = points.filter(_._1 > value)

    if (lessOrEqual.isEmpty) points.toArray.sortBy(_._1).head._2
    else if (greater.isEmpty) points.toArray.sortBy(_._1).reverse.head._2
    else{
      val low  = points.filter(_._1 <= value).toArray.reverse.head
      val high = points.filter(_._1 > value).head

      val distToLow: Temperature = value-low._1
      val distToHigh: Temperature = high._1 - value

      val lowWeight = (distToHigh / (distToLow + distToHigh))
      val highWeight= (distToLow / (distToLow + distToHigh))

      def interpolate(v1: Double, v2: Double, w1: Double, w2: Double) =
        Math.round((w1*v1   + w2*v2).floatValue)

      Color(
        interpolate(low._2.red,   high._2.red,   lowWeight, highWeight),
        interpolate(low._2.green, high._2.green, lowWeight, highWeight),
        interpolate(low._2.blue,  high._2.blue,  lowWeight, highWeight)
      )
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    def locate(x: Int, y: Int): Location ={
      val longitude = -180 + x
      val latitude = 90 - y
      Location(latitude, longitude)
    }

    val width = 360
    val height = 180

    val colorArray = new Array[Color](width*height)
    for (x <- 0 until width; y <- 0 until height) {
      colorArray(x + width*y) =
        interpolateColor(colors, predictTemperature(temperatures, locate(x,y)))
    }

    val pixelArray: Array[Pixel] = colorArray.map(c => Pixel(c.red, c.green, c.blue, 127))

    val img = Image(width, height, pixelArray)
    img
  }

  def visualizeAnyTile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    def locate(x: Int, y: Int): Location ={
      val longitude = -180 + x
      val latitude = 90 - y
      Location(latitude, longitude)
    }

    val width = 360
    val height = 180

    val colorArray = new Array[Color](width*height)
    for (x <- 0 until width; y <- 0 until height) {
      colorArray(x + width*y) =
        interpolateColor(colors, predictTemperature(temperatures, locate(x,y)))
    }

    val pixelArray: Array[Pixel] = colorArray.map(c => Pixel(c.red, c.green, c.blue, 127))

    val img = Image(width, height, pixelArray)
    img
  }
}


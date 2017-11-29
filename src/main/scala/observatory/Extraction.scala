package observatory

import java.time.{LocalDate, Month}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.types._


/**
  * 1st milestone: data extraction
  */
object Extraction {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("GlobalTemperatures")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    def readAsAllDoubles(inputFile: String, columnNames: List[String]): DataFrame = {
      def allDoublesRow(line: List[String]): Row = {
        val typeCorrectedStations: List[Any] = line.map(_.toDouble)
        Row.fromSeq(typeCorrectedStations.toSeq)
      }
      def dfSchema(columnNames: List[String]): StructType = {
        StructType(
          columnNames.map(StructField(_, DoubleType, nullable=false))
        )
      }

      val rdd = spark.sparkContext.textFile(inputFile)
        .map(_.split(",").to[List])
        .filter(list => !list.contains(""))
        .map(allDoublesRow)

      spark.createDataFrame(rdd, dfSchema(columnNames))
    }

    // Compose schema for both files
    val stationColumns:      List[String] = List("STN", "WBAN", "lat", "lon")
    val temperatureColumns: List[String] = List("STN", "WBAN", "month", "day", "tempF")

    val df_stations = readAsAllDoubles(stationsFile, stationColumns)
    val df_temperatures = readAsAllDoubles(temperaturesFile, temperatureColumns)

    // Join the two datasets
    val df_joint = df_stations
      .join(df_temperatures, usingColumns = Seq("STN", "WBAN"))
      .drop("STN", "WBAN")

    // convert temperature from F to C
    val tempInC = df_joint
      .withColumn("tempC", (col("tempF")-lit(32))*lit(5f/9))
      .drop("tempF")

    tempInC.collect.map(r => (
      LocalDate.of(year, r(2).asInstanceOf[Double].toInt, r(3).asInstanceOf[Double].toInt),
      Location(r(0).asInstanceOf[Double], r(1).asInstanceOf[Double]),
      r(4).asInstanceOf[Temperature]
      )).toSeq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}

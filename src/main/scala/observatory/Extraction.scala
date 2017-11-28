package observatory

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
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

    // Compose schema for both files
    val stationColumns:      List[String] = List("STN", "WBAN", "lat", "long")
//    val temperaturesColumns: List[String] = List("STN", "WBAN", "month", "day", "temp")

    def stationsRow(line: List[String]): Row = {
      val typeCorrectedStations: List[Any] = line.map(_.toDouble)
      Row.fromSeq(typeCorrectedStations.toSeq)
    }
    def dfSchemaStations(columnNames: List[String]): StructType = {

      // Separate treatment of head and tail
      val columnStructFields: List[StructField] = columnNames.map(StructField(_, DoubleType, nullable=false))
      StructType(columnStructFields)
    }
    val stationSchema = dfSchemaStations(stationColumns)

    val data_stations = spark.sparkContext.textFile(stationsFile)
      .map(_.split(",").to[List])
      .filter(list => !list.contains(""))
      .map(stationsRow)

//    data_stations.take(5).foreach(println(_))
    val df_stations = spark.createDataFrame(data_stations, stationSchema)

    df_stations.show(5)

    val dummyOut: Iterable[(LocalDate, Location, Temperature)] = List()
    dummyOut
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}

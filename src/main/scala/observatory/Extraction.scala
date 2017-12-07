package observatory

import java.time.{LocalDate, Month}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import scala.io.Source.fromInputStream
/**
  * 1st milestone: data extraction
  */
object Extraction {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

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

    val stationsDF =
      fromInputStream(getClass getResourceAsStream stationsFile)
        .getLines()
        .filter(! _.split(",").contains(""))
        .map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt,
                      line.split(",")(2).toDouble, line.split(",")(3).toDouble))
        .toList
        .toDF("STN", "WBAN", "lat", "lon")

    val temperaturesDF =
      fromInputStream(getClass getResourceAsStream temperaturesFile)
        .getLines()
        .filter(! _.split(",").contains(""))
        .map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt,
                      line.split(",")(2).toDouble, line.split(",")(3).toDouble, line.split(",")(4).toDouble))
        .toList
        .toDF("STN", "WBAN", "month", "day", "tempInF")


    val df: DataFrame = sparkLocateTemperatures(year, stationsDF, temperaturesDF)

    df.collect.map(r => (
      LocalDate.of(year, r(2).asInstanceOf[Double].toInt, r(3).asInstanceOf[Double].toInt),
      Location(r(0).asInstanceOf[Double], r(1).asInstanceOf[Double]),
      r(4).asInstanceOf[Temperature]
      )).toSeq
//    List()
  }

  def sparkLocateTemperatures(year: Year, stationsDF: DataFrame, temperaturesDF: DataFrame): DataFrame = {

//    // Compose schema for both files
//    val stationColumns:     List[String] = List("STN", "WBAN", "lat", "lon")
//    val temperatureColumns: List[String] = List("STN", "WBAN", "month", "day", "tempInF")
//
//    def csv_to_df(path: String, columns: List[String]): DataFrame = {
//      val schema = StructType(columns.map(StructField(_, DoubleType, nullable=false)))
//      spark.read.schema(schema).csv(path)
//    }
//
//    val df_stations = csv_to_df(stationsFile, stationColumns)
//    val df_temperatures = csv_to_df(temperaturesFile, temperatureColumns)

    // Join the two dataframes
    val df_joint = stationsDF
      .join(temperaturesDF, usingColumns = Seq("STN", "WBAN"))
      .drop("STN", "WBAN")

    // convert temperature from F to C
    val tempInC: DataFrame = df_joint
      .withColumn("tempInC", (col("tempInF")-lit(32))*lit(5f/9))
      .drop("tempInF")

    tempInC
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    List()
//    sparkAverageRecords(records.toList.toDS).collect().toSeq
  }

  def sparkAverageRecords(records: Dataset[(LocalDate, Location, Temperature)]): Dataset[(Location, Temperature)] = {
    records.groupByKey(_._2).agg(avg($"tempInC").as[Double])
  }

}

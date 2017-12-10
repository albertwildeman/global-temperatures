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
        .filter(_.split(",").length==4)
        .map(line => (line.split(",")(0),
                      line.split(",")(1),
                      line.split(",")(2).toDouble,
                      line.split(",")(3).toDouble))
        .toList.toDF("STN", "WBAN", "lat", "lon")

    val temperaturesDF =
      fromInputStream(getClass getResourceAsStream temperaturesFile)
        .getLines()
        .filter(_.split(",").length==5)
        .map(line => (line.split(",")(0),
                      line.split(",")(1),
                      line.split(",")(2).toDouble,
                      line.split(",")(3).toDouble,
                      line.split(",")(4).toDouble))
        .toList.toDF("STN", "WBAN", "month", "day", "tempInF")

    val df: DataFrame = sparkLocateTemperatures(year, stationsDF, temperaturesDF)

    df.collect.map(r => (
      LocalDate.of(year, r(2).asInstanceOf[Double].toInt, r(3).asInstanceOf[Double].toInt),
      Location(r(0).asInstanceOf[Double], r(1).asInstanceOf[Double]),
      r(4).asInstanceOf[Temperature]
      )).toSeq
  }

  def sparkLocateTemperatures(year: Year, stationsDF: DataFrame, temperaturesDF: DataFrame): DataFrame = {

    // Join the two dataframes
    val df_joint = stationsDF
      .join(temperaturesDF, usingColumns = Seq("STN", "WBAN"))
      .drop("STN", "WBAN")

    // convert temperature from F to C
    val df_tempInC: DataFrame = df_joint
      .withColumn("tempInC", (col("tempInF")-lit(32))*lit(5f)/lit(9))
      .drop("tempInF")

    df_tempInC
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
//    val records_DF = records
//      .map(tuple => (tuple._2, tuple._3))
//      .toList.toDS()
//    sparkAverageRecords(records_DF).collect().toSeq

    val recordsArray = records.toArray[(LocalDate, Location, Temperature)]
    var M = scala.collection.mutable.Map[Location, (Temperature, Int)]()
    val nRecords: Int = recordsArray.size
    var iRecord: Int = 0

    while (iRecord < nRecords){
      val loc  = recordsArray(iRecord)._2
      val temp = recordsArray(iRecord)._3

      if (M.contains(loc)){
        M(loc) = (M(loc)._1 + temp, M(loc)._2 + 1)
      }
      else {
        val n = (temp, 1)
        M += (loc -> n)
      }
      iRecord += 1
    }
    M.mapValues{case (tempSum, tempCount) => tempSum/tempCount}.toSeq
  }

  def sparkAverageRecords(records: Dataset[(Location, Temperature)]): Dataset[(Location, Temperature)] = {
//    records.groupBy("_1").avg("_2").as[(Location, Temperature)]
    records.groupByKey(_._1).agg(avg($"_2").as[Double])

//    records
//      .groupByKey(_._1)
//      .mapGroups((k, vs) => (k, vs.foldLeft(0.0,0)((acc, v) => (acc._1+v._2, acc._2+1))))
//      .map(tup => (tup._1, (tup._2._1/tup._2._2)))



//    records.groupByKey(_._1)
//      .mapValues(tup => (tup._1, tup._2, 1))
//      .reduceGroups((a: (Location, Temperature, Int), b: (Location, Temperature, Int)) => (a._1, a._2+b._2, a._3+b._3))
//        .map(tup => (tup._1, tup._2._2 / tup._2._3))
//      .as[(Location, Temperature)]

  }

}

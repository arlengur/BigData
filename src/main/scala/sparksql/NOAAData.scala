package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

import scalafx.application.JFXApp

/**
  * @author satovritti
  */
object NOAAData extends JFXApp {
  System.setProperty("hadoop.home.dir", "C:\\repo\\BigData\\lib")
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val tSchema = StructType(Array(
    StructField("sid", StringType),
    StructField("date", DateType),
    StructField("mtype", StringType),
    StructField("value", DoubleType)
  ))
  val data2017 = spark.read.schema(tSchema).option("dateFormat", "yyyyMMdd").csv("data/2017.csv")
  data2017.show()
  data2017.schema.printTreeString()


  val sSchema = StructType(Array(
    StructField("sid", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("name", StringType)
  ))

  val staionRDD = spark.sparkContext.textFile("data/ghcnd-stations.txt").map{ line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).toDouble
    val lon = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lat, lon, name)
  }
  val stations = spark.createDataFrame(staionRDD, sSchema)

  data2017.createOrReplaceTempView("data2017")
  val pureSQL1 = spark.sql(
    """
      |SELECT *
      |FROM
      |(SELECT sid, date, value as tmax FROM data2017 WHERE mtype="TMAX" LIMIT 1000)
      |JOIN
      |(SELECT sid, date, value as tmin FROM data2017 WHERE mtype="TMIN" LIMIT 1000)
      |USING (sid, date)
    """.stripMargin)
  val pureSQL2 = spark.sql(
    """
      |SELECT sid, date, (tmax+tmin)/20*1.8+32 as tave
      |FROM
      |(SELECT sid, date, value as tmax FROM data2017 WHERE mtype="TMAX" LIMIT 1000)
      |JOIN
      |(SELECT sid, date, value as tmin FROM data2017 WHERE mtype="TMIN" LIMIT 1000)
      |USING (sid, date)
    """.stripMargin)
  pureSQL2.show()

//  val tmax = data2017.filter($"mtype" === "TMAX").limit(1000).drop("mtype").withColumnRenamed("value", "tmax")
//  val tmin = data2017.filter('mtype === "TMIN").drop("mtype").withColumnRenamed("value", "tmin")
//  val combinedTemps1 = tmax.join(tmin, tmax("sid") === tmin("sid") && tmax("date") === tmin("date"))
//  val combinedTemps2 = tmax.join(tmin, Seq("sid", "date"))
//  val averageTemp = combinedTemps2.select('sid, 'date, ('tmax + 'tmin) / 20 * 1.8 + 32)
//                                  .withColumnRenamed("((((tmax + tmin) / 20) * 1.8) + 32)", "tave")
//  val stationTemp = averageTemp.groupBy('sid).agg(avg('tave))
//
//  val joinedData = stationTemp.join(stations, "sid")
//  val localData = joinedData.collect()
//
//  val temps = localData.map(_.getDouble(1))
//  val lats = localData.map(_.getDouble(2))
//  val lons = localData.map(_.getDouble(3))
//
//  val cg = ColorGradient(0.0 -> BlueARGB, 50.0 -> GreenARGB, 100.0 -> RedARGB)
//  val plot = Plot.scatterPlot(lons, lats, "Global Temps", "Longitude", "Latitude", 3.0, temps.map(cg))
//  FXRenderer(plot, 800, 600)

  spark.stop()

}

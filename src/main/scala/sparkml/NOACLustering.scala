package sparkml

import org.apache.spark.sql.{Row, SparkSession}
import sparksql.NOAAData.spark
import swiftvis2.plotting.Plot

/**
  * @author satovritti
  */

case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)

object NOACLustering {
  System.setProperty("hadoop.home.dir", "C:\\repo\\BigData\\lib")
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val stations = spark.read.textFile("data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).trim.toDouble
    val lon = line.substring(21, 30).trim.toDouble
    val elev = line.substring(31, 37).trim.toDouble
    val name = line.substring(41, 71)
    Station(id, lat, lon, elev, name)
  }.cache()

  val x = stations.select('lon).as[Double].collect()
  val y = stations.select('lat).as[Double].collect()
//  val plot = Plot.scatterPlot(x, y, title = "Stations", xLabel = "Longitude", yLabel = "Latitude", symbolSize = 3.0, temps.map(cg))

  import scala.io.Source
  object LongLines {
    def processFile(filename: String, width: Int) {
      def processLine(line: String) {
        if (line.length > width)
          println(filename +": "+ line)
      }
      val source = Source.fromFile(filename)
      for (line <- source.getLines())
        processLine(line)
    }
  }
      spark.stop()

}

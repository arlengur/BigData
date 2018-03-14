package standardscala

import standardscala.TempData.toDoubleOrNeg

import scalafx.application.JFXApp
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.chart.{NumberAxis, ScatterChart, XYChart}
import swiftvis2.plotting
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.FXRenderer
import swiftvis2.plotting.ColorGradient

/**
  * @author satovritti
  */
object PlotTemps extends JFXApp {
  val source = scala.io.Source.fromFile("MN212142_9392.csv")
  val lines = source.getLines().drop(1)
  val data = lines.flatMap { line =>
    val p = line.split(",")
    if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
      Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble,
        p(8).toDouble, p(9).toDouble))
  }.toArray

  val rainData = data.filter(_.precip >= 0.0).sortBy(_.precip)
  val cg = ColorGradient((0.0, 0xFF000000), (1.0, 0xFF00FF00), (10.0, 0xFF0000FF))
  val plot = Plot.scatterPlot(rainData.map(_.doy), rainData.map(_.tmax), "Temps", "Day of Year",
    "Temp", 5.0, rainData.map(td => cg(td.precip)))
  FXRenderer(plot, 500, 500)

//  val plot = Plot.scatterPlot(data.map(_.doy), data.map(_.tmax), "Temps", "day of year", "temp")
//  FXRenderer(plot, 500, 500)

//  stage = new JFXApp.PrimaryStage {
//    title = "Temp plot"
//    scene = new Scene(500, 500) {
//      val xAxis = NumberAxis()
//      val yAxis = NumberAxis()
//      val pData = XYChart.Series[Number, Number]("Temps",
//        ObservableBuffer(data.map(td => XYChart.Data[Number, Number](td.doy, td.tmax)):_*))
//      val plot = new ScatterChart(xAxis, yAxis, ObservableBuffer(pData))
//      root = plot
//    }
//  }
}

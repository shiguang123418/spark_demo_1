import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.chart.ui.ApplicationFrame
import redis.clients.jedis.Jedis

import java.awt.Font
import javax.swing.{SwingUtilities, JFrame}
import java.util.concurrent.{ScheduledExecutorService, Executors, TimeUnit}

class BarChartExample extends ApplicationFrame("JFreeChart 柱状图示例") {
  private var chartPanel: ChartPanel = _
  def createChart(): Unit = {
    val jedis = new Jedis("localhost", 6379)
    val cityKeys = jedis.keys("orders:citymoney:*").toArray.map(_.toString)
    println(s"Redis city keys: ${cityKeys.mkString(", ")}")
    val dataset = new DefaultCategoryDataset()
    cityKeys.foreach { key =>
      val province = jedis.hget(key, "province") // 获取城市名
      val moneyStr = jedis.hget(key, "money")   // 获取销售额

      if (province != null && moneyStr != null) {
        try {
          val money = moneyStr.toDouble
          dataset.addValue(money, "销售额", province)
        } catch {
          case e: NumberFormatException =>
            println(s"Invalid money value for city $province: $moneyStr")
        }
      }
    }
    jedis.close()

    val chart = ChartFactory.createBarChart(
      "产品销售比较",
      "城市",
      "销售额",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )

    val titleFont = new Font("宋体", Font.BOLD, 20)
    chart.getTitle.setFont(titleFont)
    val axisLabelFont = new Font("宋体", Font.PLAIN, 12)
    val categoryAxis = chart.getCategoryPlot.getDomainAxis
    categoryAxis.setLabelFont(axisLabelFont)
    chart.getCategoryPlot.getDomainAxis().setTickLabelFont(axisLabelFont)
    chart.getCategoryPlot.getRangeAxis().setLabelFont(axisLabelFont)

    if (chartPanel != null) {
      chartPanel.setChart(chart)
    } else {
      chartPanel = new ChartPanel(chart)
      chartPanel.setFillZoomRectangle(true)
      chartPanel.setMouseWheelEnabled(true)
      setContentPane(chartPanel)
    }
  }
  def startAutoRefresh(): Unit = {
    val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    scheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        SwingUtilities.invokeLater(new Runnable {
          override def run(): Unit = {
            createChart()
            pack()
          }
        })
      }
    }, 0, 2, TimeUnit.SECONDS)
  }
}

object BarChartExample {
  def main(args: Array[String]): Unit = {
    SwingUtilities.invokeLater(new Runnable {
      override def run(): Unit = {
        val example = new BarChartExample()
        example.createChart()
        example.startAutoRefresh()
        example.pack()
        example.setVisible(true)
      }
    })
  }
}

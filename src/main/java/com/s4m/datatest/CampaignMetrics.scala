package com.s4m.datatest

import com.s4m.datatest.util.ConfigReader
import com.s4m.datatest.Constants.{TARGET_PATH}
import org.apache.spark.sql.SparkSession

object CampaignMetrics{

  def main(args: Array[String]): Unit = {

    /**
     * select auctionId,sum(event) from EVENT_METRICS where application = 'PayPal' and event = 'Impression'
     * and timestamp between '2021-12-01 00:00:00' and '2021-12-31 23:59:59'
     */

    val query: String = args(0)

    val properties = ConfigReader.readConfig("campaignMetrics")

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val inputDf = sparkSession
      .read
      .load(properties.getProperty(TARGET_PATH))

    inputDf.createGlobalTempView("EVENT_METRICS")

    val impressionMetrics = sparkSession.sql(query)

    impressionMetrics.show(false)
  }

}

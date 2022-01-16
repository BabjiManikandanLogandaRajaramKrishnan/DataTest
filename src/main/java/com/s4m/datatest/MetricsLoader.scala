package com.s4m.datatest

import java.util.concurrent.TimeUnit

import com.s4m.datatest.entity.{Context, Event}
import com.s4m.datatest.Constants.{CONTEXT_SOURCE_PATH, EVENT_SOURCE_PATH, TARGET_PATH}
import com.s4m.datatest.util.{ConfigReader, Converters}
import org.apache.spark.sql.functions.{col, datediff, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

object MetricsLoader extends App {

  val sparkSession = SparkSession.builder().master("local").getOrCreate()
  val converters = new Converters()

  import sparkSession.implicits._

  val properties = ConfigReader.readConfig("campaignMetrics")

  val contextDf : Dataset[Context] = sparkSession
    .readStream
    .load(properties.getProperty(CONTEXT_SOURCE_PATH))
    .as[Context]

  val eventDf : Dataset[Event] = sparkSession
    .readStream
    .load(properties.getProperty(EVENT_SOURCE_PATH))
    .as[Event]

  val contextDfWithWatermark = contextDf
    .withColumnRenamed("timeStamp", "eventTimeStamp")
    .withWatermark("timeStamp", "5 minutes")

  val eventDfWithWatermark = eventDf.withWatermark("timeStamp", "5 minutes")

  val contextDf1 = contextDf
    .withColumn("timeInterval",
      converters.convertUDF(col("timeStamp")))

  val joinedDf = contextDf
    .join(eventDf,
      contextDf.col("auctionId").equalTo(eventDf.col("auctionId"))
        .and(datediff(eventDf.col("timeStamp"),contextDf.col("timeInterval")).leq(1)))

  val flattenedDf = joinedDf
    .select("auctionId, eventTimestamp, publisher, application, country, userId, campaignId, event")
    .withColumn("partitionYear", converters.parseDate(col("eventTimestamp"), lit("YYYY")))
    .withColumn("partitionMonth", converters.parseDate(col("eventTimestamp"), lit("MM")))
    .withColumn("partitionDate", converters.parseDate(col("eventTimestamp"), lit("DD")))


  val writeDf = joinedDf
    .writeStream
    .format("parquet")
    .outputMode("append")
    .partitionBy("partitionYear","partitionMonth","partitionDate")
    .option("checkpointLocation","s3://checkpoint/dir")
    .option("path", TARGET_PATH)
    .trigger(Trigger.ProcessingTime(60, TimeUnit.MINUTES))
    .start()

  writeDf.awaitTermination()

}

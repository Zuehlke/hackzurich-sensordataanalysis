package com.zuehlke.hackzurich.service

import java.text.SimpleDateFormat

import akka.actor.Actor
import com.zuehlke.hackzurich.common.dataformats.{AccelerometerReadingJSON4S, Prediction}
import com.zuehlke.hackzurich.service.PredictionActor.RequestPrediction
import com.zuehlke.hackzurich.service.SparkDataAnalyticsPollingActor.SparkAnalyticsData
import com.zuehlke.hackzurich.service.SpeedLayerKafkaPollingActor.SpeedLayerData

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class PredictionActor extends Actor {
  private val sparkAnalyticsData = mutable.Map.empty[String, Prediction]
  private val speedLayerData = ArrayBuffer.empty[AccelerometerReadingJSON4S]
  private val speedLayerDataMap = mutable.Map.empty[String, ArrayBuffer[(Long, Double)]]

  private val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * How far in the future we want to predict
    */
  val forecastTime = 5

  private def compilePredictions(): Seq[Prediction] = {
    val speedLayerBuffer = ArrayBuffer.empty[Prediction]

    for (device <- speedLayerDataMap.keySet) {
      val values = ArrayBuffer.empty[Double]
      val timestamps = ArrayBuffer.empty[Long]
      for (entry <- speedLayerDataMap(device)) {
        values += entry._2
        timestamps += entry._1
      }
      speedLayerBuffer += Prediction(timestamps.min, device, values)
    }

    val result = sparkAnalyticsData.values.toSeq ++ speedLayerBuffer

    println(s"PredictionActor: speedLayerData: $speedLayerData")
    println(s"PredictionActor: speedLayerDataMap: $speedLayerDataMap")
    println(s"PredictionActor: speedLayerBuffer: $speedLayerBuffer")
    println(s"PredictionActor: sparkAnalyticsData: $sparkAnalyticsData")
    println(s"PredictionActor: result: $result")

    speedLayerData.clear()
    result
  }

  private def updateSpeedLayerMap(): Unit = {
    for (x <- speedLayerData) {
      val id = x.deviceid
      // Create a new ArrayBuffer if there is no data yet for a specific deviceid
      val lst = speedLayerDataMap.getOrElse(id, ArrayBuffer.empty[(Long, Double)])
      // we can add it to the end of the list, as we know that the timestamps are in increasing order
      lst += Tuple2(x.date, x.z)
      speedLayerDataMap += (id -> lst)
    }
    speedLayerData.clear()
  }

  /**
    * Forecasts `forecastTime` values based on linear Regression
    *
    * @param values y values
    * @return Sequence of Doubles (size `forecastTime`)
    */
  private def forecast(values: Seq[Double]): Seq[Double] = {
    require(values.length >= 2, "I cannot predict something from less than two values")

    val lastX = values.length.toDouble
    val range = 1.0 to(lastX, 1.0)
    val regression = new LinearRegression(range, values)
    val predictions = for (i <- lastX + 1 to(lastX + forecastTime, 1.0)) yield regression.predict(i)
    predictions
  }

  override def receive: Receive = {
    case x: SparkAnalyticsData =>
      sparkAnalyticsData ++= x.data
    case x: SpeedLayerData =>
      speedLayerData ++= x.data
      updateSpeedLayerMap()
    case RequestPrediction() =>
      sender() ! predictionsToString()
    case x => println(s"PredictionActor: I got a weird message: $x")
  }

  private def predictionsToString(): String = {
    sparkDataToString + forecastedSpeedLayerDataToString + speedLayerDataToString
  }

  private def sparkDataToString: String = {
    val sb = new mutable.StringBuilder()
    sb.append("                 SparkDataAnalytics Data              \n")
    sb.append("======================================================\n")

    for (device <- sparkAnalyticsData.keySet) {
      sb.append(device).append("\n")
      var i = 0
      val p = sparkAnalyticsData(device)
      for (value <- p.values) {
        sb.append(timeFormatter.format(p.timestamp + i))
        sb.append(" - - - - ")
        sb.append(value)
        sb.append("\n")
        i += 1000
      }
      sb.append("-------------------------------------------\n\n")
    }
    sb.append("\n\n")
    sb.toString()
  }

  private def speedLayerDataToString: String = {
    val sb = new mutable.StringBuilder()
    sb.append("                     Speed Layer Data                 \n")
    sb.append("======================================================\n")

    for (device <- speedLayerDataMap.keySet) {
      sb.append(device).append("\n")
      val p = speedLayerDataMap(device)
      for (value <- p) {
        sb.append(timeFormatter.format(value._1))
        sb.append(" - - - - ")
        sb.append(value._2)
        sb.append("\n")
      }
      sb.append("-------------------------------------------\n\n")
    }
    sb.append("\n\n")
    sb.toString()
  }

  private def forecastedSpeedLayerDataToString: String = {
    val sb = new mutable.StringBuilder()
    sb.append("               FORECASTED Speed Layer Data            \n")
    sb.append("======================================================\n")

    for (device <- speedLayerDataMap.keySet) {
      sb.append(device).append("\n")

      val p = speedLayerDataMap(device)
      val values = for (x <- p) yield x._2
      val lastTimestamp = (for (x <- p) yield x._1).max

      val predictions = forecast(values)
      var i = 1000
      for (value <- predictions) {
        sb.append(timeFormatter.format(lastTimestamp + i))
        sb.append(" - - - - ")
        sb.append(value)
        sb.append("\n")
        i += 1000
      }
      sb.append("-------------------------------------------\n\n")
    }
    sb.append("\n\n")
    sb.toString()
  }


}


object PredictionActor {

  case class RequestPrediction()

}
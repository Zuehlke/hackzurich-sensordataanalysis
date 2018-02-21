package com.zuehlke.hackzurich.service

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

  /**
    * How far in the future we want to predict
    */
  val forecastTime = 5

  def compilePredictions(): Seq[Prediction] = {
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
      val id = s"FAST LAYER - ${x.deviceid}" // TODO: DEBUG ONLY!!!!
      // Create a new ArrayBuffer if there is no data yet for a specific deviceid
      val lst = speedLayerDataMap.getOrElse(id, ArrayBuffer.empty[(Long, Double)])
      // we can add it to the end of the list, as we know that the timestamps are in increasing order
      lst += Tuple2(x.date, x.z)
      speedLayerDataMap += (id -> lst)
    }
  }

  /**
    * Forecasts `forecastTime` values based on linear Regression
    *
    * @param values y values
    * @return Sequence of Doubles (size `forecastTime`)
    */
  def forecast(values: Seq[Double]): Seq[Double] = {
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
      sender() ! compilePredictions()
    case x => println(s"PredictionActor: I got a weird message: $x")
  }
}


object PredictionActor {

  case class RequestPrediction()

}
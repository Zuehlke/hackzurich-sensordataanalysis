package com.zuehlke.hackzurich.service

import akka.actor.Actor
import com.zuehlke.hackzurich.common.dataformats.{AccelerometerReadingJSON4S, Prediction}
import com.zuehlke.hackzurich.service.PredictionActor.RequestPrediction
import com.zuehlke.hackzurich.service.SparkDataAnalyticsPollingActor.SparkAnalyticsData
import com.zuehlke.hackzurich.service.SpeedLayerKafkaPollingActor.SpeedLayerData

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class PredictionActor extends Actor {
  val sparkAnalyticsData = mutable.Map.empty[String, Prediction]
  val speedLayerData = mutable.ArrayBuffer.empty[AccelerometerReadingJSON4S]

  def predict(): Seq[Prediction] = {
    val buffer = ArrayBuffer.empty[Prediction]
    for (x <- speedLayerData) {
      buffer += Prediction(x.date, x.deviceid, Seq(x.z))
    }
    val result = sparkAnalyticsData.values.toSeq ++ buffer

    println(s"PredictionActor: speedLayerData: $speedLayerData")
    println(s"PredictionActor: buffer: $buffer")
    println(s"PredictionActor: sparkAnalyticsData: $sparkAnalyticsData")
    println(s"PredictionActor: result: $result")

    result
  }

  override def receive: Receive = {
    case x: SparkAnalyticsData =>
      sparkAnalyticsData ++= x.data
    case x: SpeedLayerData =>
      speedLayerData ++= x.data
    case RequestPrediction() =>
      sender() ! predict()
    case x => println(s"PredictionActor: I got a weird message: $x")
  }
}


object PredictionActor {

  case class RequestPrediction()

}
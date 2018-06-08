package com.zuehlke.hackzurich

import java.util.Properties

import org.apache.spark.sql.DataFrame

/**
  * An instance of this trait is supposed to provide a concrete implementation of the
  * prediction, based on an input DataFrame and Kafka Producer Properties, which
  * are used to publish the results.
  */
trait IPrediction {

  /**
    * Performs the prediction.
    * This method is usually called in a loop.
    *
    * @param dataFrame          Serves as input and is prefilled with data
    * @param producerProperties The properties are used to publish the results to a Kafka topic
    */
  def performPrediction(dataFrame: DataFrame, producerProperties: Properties): Unit
}

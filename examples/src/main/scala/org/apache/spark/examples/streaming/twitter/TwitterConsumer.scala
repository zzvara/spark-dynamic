
package org.apache.spark.examples.streaming.twitter

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import twitter4j.Status

object TwitterConsumer {
  def main(args: Array[String]) {
    val batchDuration = args(3).toInt
    val reducerLoad = args(4).toDouble

    val sparkConf = new SparkConf()
      .setAppName("Streaming Twitter Consumer")
      .setJars(Seq(args(1)))
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))

    val records =
      KafkaUtils.createDirectStream[String, Status](
        ssc,
        LocationStrategies.PreferBrokers,
        ConsumerStrategies.Subscribe[String, Status](
          Seq("twitter"),
          Map[String, Object](
            "group.id" -> args(0),
            "bootstrap.servers" -> "hadoop00:9092",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.spark.examples.streaming.twitter.TweetDeserializer"
          ),
          (0 to 3).map(i => (new TopicPartition("twitter", i), 0L)).toMap
        )
      )
      .flatMap(pair => pair.value().getHashtagEntities.map(
        tag => (tag.getText, (tag.getText, pair.value()))
      ))

    records
      .groupByKey()
      .map {
        x => {
          x
        }
      }
      /**
        * This ensures that the groping is correct in each mini-batch!
        */
      .map {
      group => group.ensuring(
        _._2.map(_._1).forall(x => group._1 == x), "False!")
    }
    .count()
    .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
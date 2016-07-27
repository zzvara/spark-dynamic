
package org.apache.spark.examples.streaming.twitter

import kafka.serializer.StringDecoder
import org.apache.spark.AccumulatorParam.Weightable
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import twitter4j.Status

object TwitterConsumer {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Streaming Twitter Consumer")
      .setJars(Seq(args(1)))
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams: Map[String, String] = Map(
      "group.id" -> args(0),
      "zookeeper.connect" -> "localhost:2181",
      "auto.offset.reset" -> "smallest"
    )
    val topics = Map("twitter" -> 1)

    val records =
      KafkaUtils.createStream[String, Status, StringDecoder, StatusDecoder](
        ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY
      )
      .flatMap(pair => pair._2.getHashtagEntities.map(tag => (tag.getText, (tag.getText, pair._2))))

    records
      .groupByKey()
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
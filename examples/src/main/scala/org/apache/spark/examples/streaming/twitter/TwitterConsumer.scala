
package org.apache.spark.examples.streaming.twitter

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import twitter4j.Status

/**
  * VM OPTIONS
  * -Dspark.master=local[10]
-Dspark.executor.instances=6
-Dspark.executor.cores=4
-Dspark.executor.memory=6g
-Dspark.repartitioning=true
-Dspark.shuffle.sort.bypassMergeThreshold=1
-Dspark.repartitioning.partitioner-tree-depth=8
-Dspark.repartitioning.throughput.interval=12000
-Dspark.repartitioning.histogram-threshold=1
-Dspark.data-characteristics.backoff-factor=2.0
-Dspark.data-characteristics.histogram-scale-boundary=20
-Dspark.data-characteristics.histogram-size-boundary=100
-Dspark.data-characteristics.histogram-compaction=60
-Dspark.shuffle.write.data-characteristics=true
-Dspark.streaming.backpressure.enabled=true
-Dspark.streaming.kafka.maxRatePerPartition=2000
-Dlog4j.configuration=file:\\\Users\Ehnalis\Projects\dynamic-repartitioning\conf\log4j.properties
-Dspark.repartitioning.streaming.per-batch-sampling-rate=1
-Dspark.streaming.kafka.consumer.poll.ms=2000

  ARGUMENTS
  group92
C:\Users\Ehnalis\Projects\dynamic-repartitioning\examples\target\scala-2.11\jars\spark-examples_2.11-2.1.0-SNAPSHOT.jar
2
5
0.001
  */

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
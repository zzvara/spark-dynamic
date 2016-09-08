
package org.apache.spark.examples.streaming.twitter

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import twitter4j.Status

object TwitterConsumer extends Logging {
  type Options = Map[Symbol, Any]

  def main(arguments: Array[String]) {
    val options = parseOptions(arguments.toList)

    val configuration = new SparkConf()
      .setAppName("Streaming Twitter Consumer")
      .setJars(options('jars).asInstanceOf[Seq[String]])
    val context = new StreamingContext(configuration, Seconds(options('batchDuration).asInstanceOf[Int]))

    val kafkaGroupPostfix = System.currentTimeMillis().toString
    logInfo(s"Kafka group postfix is going to be [$kafkaGroupPostfix], " +
            s"just in case you want to continue with the group manually.")

    val records =
      KafkaUtils.createDirectStream[String, Status](
        context,
        LocationStrategies.PreferBrokers,
        ConsumerStrategies.Subscribe[String, Status](
          Seq("twitter"),
          Map[String, Object](
            "group.id" ->
              (options('kafkaGroupPrefix).toString + kafkaGroupPostfix),
            "bootstrap.servers" -> options('kafkaServers).toString,
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.spark.examples.streaming.twitter.TweetDeserializer"
          ),
          (0 to 3).map(
            i => (new TopicPartition("twitter", i), options('uniformKafkaOffset).toString.toLong)
          ).toMap
        )
      )
      .flatMap(pair => pair.value().getHashtagEntities.map(
        tag => (tag.getText, (tag.getText, pair.value()))
      ))

    records
      .groupByKey(options('defaultGroupByPartitions).toString.toInt)
      .map {
        x => x
      }
      /**
        * This ensures that the groping is correct in each mini-batch!
        */
      .map {
      group => group.ensuring(
        _._2.map(_._1).forall(x => group._1 == x), "Incorrect grouping!")
      }
    .count()
    .print()

    context.start()

    /**
      * Listener is going to switch off the streaming job if the source goes empty.
      */
    context.addStreamingListener(new StreamingListener {
      var noRecordsCount = 0
      /**
        * Called when processing of a batch of jobs has completed.
        */
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        if (batchCompleted.batchInfo.numRecords == 0) {
          noRecordsCount += 1
          logWarning(s"Empty batch detected ombre, $noRecordsCount times!")
        } else {
          noRecordsCount = 0
        }
        if (noRecordsCount > options('emptyBatchLimit).toString.toInt) {
          logError("Too many empty batches bitches!")
          context.stop(stopSparkContext = true, stopGracefully = false)
        }
      }
    })
    context.awaitTermination()
  }

  def parseOptions(arguments: List[String]): Options = {
    def nextOption(map : Options, list: List[String]) : Options = {
      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map
        case "--kafkaServers" :: value :: tail =>
          nextOption(map ++ Map('kafkaServers -> value), tail)
        case "--kafkaGroupPrefix" :: value :: tail =>
          nextOption(map ++ Map('kafkaGroupPrefix -> value), tail)
        case "--defaultGroupByPartitions" :: value :: tail =>
          nextOption(map ++ Map('defaultGroupByPartitions -> value.toInt), tail)
        case "--jars" :: value :: tail =>
          nextOption(map ++ Map('jars -> value.toString.split(",").toSeq), tail)
        case "--batchDuration" :: value :: tail =>
          nextOption(map ++ Map('batchDuration -> value.toInt), tail)
        case "--uniformKafkaOffset" :: value :: tail =>
          nextOption(map ++ Map('uniformKafkaOffset -> value.toInt), tail)
        case "--emptyBatchLimit" :: value :: tail =>
          nextOption(map ++ Map('emptyBatchLimit -> value.toInt), tail)
        case option :: tail =>
          logError(s"Unknown option '$option'!")
          throw new IllegalArgumentException("Unknown option!")
      }
    }

    nextOption(Map[Symbol, Any](), arguments)
  }
}
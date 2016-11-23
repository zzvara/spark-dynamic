
package org.apache.spark.examples.streaming.twitter

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object Downloader extends Logging {
  type Options = Map[Symbol, Any]

  def main(arguments: Array[String]) {
    val options = parseOptions(arguments.toList)

    val configuration = new SparkConf()
      .setAppName("I'm downloading Twitter data from a remote Kafka, bro'. Chill.")
      .setJars(options('jars).asInstanceOf[Seq[String]])
    val context = new SparkContext(configuration)

    val kafkaGroupPostfix = System.currentTimeMillis().toString
    logInfo(s"Kafka group postfix is going to be [$kafkaGroupPostfix], " +
            s"just in case you want to continue with the group manually.")
    val kafkaGroup = options('kafkaGroupPrefix).toString + kafkaGroupPostfix

    var batch = kafkaBatch(context, options, kafkaGroup)
    var batches = List[RDD[(Long, Status)]]()
    batches = batches :+ batch._1
    var totalNumberOfRecords = batch._2
    logInfo("Finished initial batch.")
    var batchNumber = 1
    var records = batch._1
    val isTestRun = options('testRun).toString.toBoolean
    while(batch._2 > 0 && (!isTestRun || batchNumber < 3)) {
      batch = kafkaBatch(context, options, kafkaGroup)
      batchNumber += 1
      totalNumberOfRecords += batch._2
      logInfo(s"Total number of fetched records is $totalNumberOfRecords.")
      logInfo(s"Finished ${batchNumber}th batch. Creating a union.")
      batches = batches :+ batch._1
      records = records.union(batch._1)
    }

    val cached = records.cache()

    logInfo(s"Count after cache is ${records.count()}.")

    logInfo(s"Total number of fetched records is $totalNumberOfRecords.")
    logInfo("Sorting and writing the data out.")

    val count = cached
      .sortByKey(ascending = true, 1)
      .mapPartitions {
        iterator => {
          println("Mapping your partition's ass.")

          val props = System.getProperties
          props.put("bootstrap.servers", options('toKafkaServers).toString)
          props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer",
                    "org.apache.spark.examples.streaming.twitter.TweetSerializer")

          val producer = new KafkaProducer[String, Status](props)

          iterator.foreach { status =>
            val data = new ProducerRecord[String, Status]("goodTwitter", status._2)
            producer.send(data)
          }

          producer.close(10, TimeUnit.SECONDS)

          Iterator.empty
        }
      }
      .count()

    logInfo(s"Final count is $count.")
  }

  def kafkaBatch(context: SparkContext,
                 options: Options,
                 kafkaGroup: String): (RDD[(Long, Status)], Long) = {
    val batch = context
      .parallelize(1 to 100, options('parallelism).toString.toInt)
      .mapPartitions {
        trashData => kafkaRead(options, kafkaGroup)
      }
      .persist(StorageLevel.DISK_ONLY)

    (batch, batch.count())
  }

  def kafkaRead(options: Options, kafkaGroup: String): Iterator[(Long, Status)] = {
    val kafkaPartitions = (0 to 3).map(
      i => new TopicPartition("twitter", i)
    )

    val props = System.getProperties
    props.put("bootstrap.servers", options('toKafkaServers).toString)
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.spark.examples.streaming.twitter.TweetDeserializer")
    props.put("bootstrap.servers", options('fromKafkaServers).toString)
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", kafkaGroup)

    val consumer = new KafkaConsumer[String, Status](props)

    consumer.subscribe(List("twitter"))

    var iterator = List[(Long, Status)]()
    var triedPolls = 0
    val pollTime = options('pollTime).toString.toInt
    var totalPolledRecords = 0
    val batchSizes = options('batchSizes).toString.toInt

    logInfo("Starting polling.")

    while(triedPolls < options('pollLimit).toString.toInt && totalPolledRecords < batchSizes) {
      val polledRecords = consumer.poll(pollTime)
      consumer.commitAsync()
      logInfo(s"Polled for $pollTime ms, ${polledRecords.count()} records, " +
              s" $totalPolledRecords in total.")
      totalPolledRecords += polledRecords.count()
      if (polledRecords.isEmpty) {
        triedPolls += 1
      } else {
        val result = polledRecords.iterator().asScala.map {
          record =>
            if (math.random < 0.001) {
              logInfo(s"Sample record with ${record.value().getCreatedAt.getTime} time.")
            }
            (record.value().getCreatedAt.getTime, record.value())
        }
        iterator = iterator ++ result
      }
    }

    logInfo(s"Finished polling, with $totalPolledRecords total number of records.")

    iterator.iterator
  }

  def parseOptions(arguments: List[String]): Options = {
    def nextOption(map : Options, list: List[String]) : Options = {
      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map
        case "--fromKafkaServers" :: value :: tail =>
          nextOption(map ++ Map('fromKafkaServers -> value), tail)
        case "--pollLimit" :: value :: tail =>
          nextOption(map ++ Map('pollLimit -> value), tail)
        case "--toKafkaServers" :: value :: tail =>
          nextOption(map ++ Map('toKafkaServers -> value), tail)
        case "--kafkaGroupPrefix" :: value :: tail =>
          nextOption(map ++ Map('kafkaGroupPrefix -> value), tail)
        case "--jars" :: value :: tail =>
          nextOption(map ++ Map('jars -> value.toString.split(",").toSeq), tail)
        case "--parallelism" :: value :: tail =>
          nextOption(map ++ Map('parallelism -> value.toInt), tail)
        case "--pollTime" :: value :: tail =>
          nextOption(map ++ Map('pollTime -> value.toInt), tail)
        case "--batchSizes" :: value :: tail =>
          nextOption(map ++ Map('batchSizes -> value.toInt), tail)
        case "--testRun" :: value :: tail =>
          nextOption(map ++ Map('testRun -> value), tail)
        case option :: tail =>
          logError(s"Unknown option '$option'!")
          throw new IllegalArgumentException("Unknown option!")
      }
    }

    nextOption(Map[Symbol, Any](), arguments)
  }
}
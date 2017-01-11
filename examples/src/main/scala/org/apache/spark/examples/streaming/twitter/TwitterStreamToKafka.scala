
package org.apache.spark.examples.streaming.twitter

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterStreamToKafka extends Logging {
  type Options = Map[Symbol, Any]

  def main(arguments: Array[String]) {
    val options = parseOptions(arguments.toList)

    var configuration = new SparkConf()
      .set("spark.executor.extraJavaOptions",
        "-Dkey.serializer=org.apache.kafka.common.serialization.StringSerializer " +
        "-Dvalue.serializer=org.apache.spark.examples.streaming.twitter.TweetSerializer " +
        "-Dbootstrap.servers=worker-1.hadoop.core:9092 " +
        "-Dtwitter4j.oauth.consumerKey=bCyiEHrI7ahCHtr2P99u3qE2z " +
        "-Dtwitter4j.oauth.consumerSecret=tqO1uSAeKXYSHjzAFxLVEkBEWnAiLOP8cWcsStbnNk1m1MGwmo " +
        "-Dtwitter4j.oauth.accessToken=1168984075-YQp3FVZm9Z2cQHYgfGCbRqJnXVO3qrEUztWuWKW " +
        "-Dtwitter4j.oauth.accessTokenSecret=EwYS83SVtxfF2Pxu4rJEKdtx4ijEKDaPsBgp4fJS2AuEM"
      )

    options.get('jars).map(_.asInstanceOf[Seq[String]]).foreach {
      jars =>
        configuration = configuration.setJars(jars)
    }

    val ssc = new StreamingContext(
      configuration,
      Seconds(options('batchDuration).asInstanceOf[Int])
    )

    ssc.checkpoint(options('checkpointDirectory).asInstanceOf[String])
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    stream
      .mapPartitions { iterator =>
        println("Mapping your partition ass.")
        val props = System.getProperties
        props.put("request.required.acks", "1")

        val producer = new KafkaProducer[String, Status](props)

        iterator.foreach { status =>
          val data = new ProducerRecord[String, Status](
            "twitter", status)
          println(status.getText)
          producer.send(data)
        }

        producer.close()

        Iterator.empty
      }
      .count()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }


  def parseOptions(arguments: List[String]): Options = {
    def nextOption(map : Options, list: List[String]) : Options = {
      list match {
        case Nil => map
        case "--checkpointDirectory" :: value :: tail =>
          nextOption(map ++ Map('checkpointDirectory -> value), tail)
        case "--jars" :: value :: tail =>
          nextOption(map ++ Map('jars -> value.toString.split(",").toSeq), tail)
        case "--batchDuration" :: value :: tail =>
          nextOption(map ++ Map('batchDuration -> value.toInt), tail)
        case option :: tail =>
          logError(s"Unknown option '$option'!")
          throw new IllegalArgumentException("Unknown option!")
      }
    }

    nextOption(Map[Symbol, Any](), arguments)
  }
}

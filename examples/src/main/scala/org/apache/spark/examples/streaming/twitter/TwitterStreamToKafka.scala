
package org.apache.spark.examples.streaming.twitter

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterStreamToKafka {
  def main(args: Array[String]): Unit = {

    val cfg = new SparkConf()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(cfg, Seconds(2))
    ssc.checkpoint(args(4))
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    stream
      .foreachRDD { rdd =>
        rdd.mapPartitions { iterator =>

          val props = new Properties()
          props.put("metadata.broker.list", "localhost:9092")
          props.put("request.required.acks", "1")
          props.put("serializer.class",
                    "org.apache.spark.examples.streaming.twitter.TweetSerializer")

          val config = new ProducerConfig(props)

          val producer = new Producer[String, Status](config)

          val iter = iterator.map { status =>
            val data = new KeyedMessage[String, Status](
              "twitter", status)
            producer.send(data)
            status
          }

          println(iter.size)

          Iterator.empty
        }
        .count()
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

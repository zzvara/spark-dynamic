
package org.apache.spark.examples.streaming.twitter

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterStreamToKafka {
  def main(args: Array[String]): Unit = {

    val cfg = new SparkConf()
      .setJars(Seq(args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8)))
      .set("spark.executor.extraJavaOptions", "-Dkey.serializer=org.apache.kafka.common.serialization.StringSerializer -Dvalue.serializer=org.apache.spark.examples.streaming.twitter.TweetSerializer -Dbootstrap.servers=master.dr:9092 -Dtwitter4j.oauth.consumerKey=bCyiEHrI7ahCHtr2P99u3qE2z -Dtwitter4j.oauth.consumerSecret=tqO1uSAeKXYSHjzAFxLVEkBEWnAiLOP8cWcsStbnNk1m1MGwmo -Dtwitter4j.oauth.accessToken=1168984075-YQp3FVZm9Z2cQHYgfGCbRqJnXVO3qrEUztWuWKW -Dtwitter4j.oauth.accessTokenSecret=EwYS83SVtxfF2Pxu4rJEKdtx4ijEKDaPsBgp4fJS2AuEM")

    val ssc = new StreamingContext(cfg, Seconds(2))
    ssc.checkpoint(args(0))
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    stream
      .mapPartitions { iterator =>
        println("Mapping your partition ass.")
        val props = System.getProperties
        props.put("request.required.acks", "1")

        val producer = new KafkaProducer[String, Status](props)

        val iter = iterator.map { status =>
          val data = new ProducerRecord[String, Status](
            "twitter", status)
          println(status.getText)
          producer.send(data)
          status
        }

        producer.flush()
        producer.close()

        Iterator.empty
      }
      .count()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

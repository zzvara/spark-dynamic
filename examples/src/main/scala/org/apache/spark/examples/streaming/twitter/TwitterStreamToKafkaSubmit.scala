
package org.apache.spark.examples.streaming.twitter

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterStreamToKafkaSubmit {
  def main(args: Array[String]): Unit = {
    val cfg = new SparkConf()
      .set("spark.executor.extraJavaOptions", "-Dkey.serializer=org.apache.kafka.common.serialization.StringSerializer -Dvalue.serializer=org.apache.spark.examples.streaming.twitter.TweetSerializer -Dbootstrap.servers=hadoop00:9092 -Dtwitter4j.oauth.consumerKey=bCyiEHrI7ahCHtr2P99u3qE2z -Dtwitter4j.oauth.consumerSecret=tqO1uSAeKXYSHjzAFxLVEkBEWnAiLOP8cWcsStbnNk1m1MGwmo -Dtwitter4j.oauth.accessToken=1168984075-YQp3FVZm9Z2cQHYgfGCbRqJnXVO3qrEUztWuWKW -Dtwitter4j.oauth.accessTokenSecret=EwYS83SVtxfF2Pxu4rJEKdtx4ijEKDaPsBgp4fJS2AuEM")

    val ssc = new StreamingContext(cfg, Seconds(2))
    ssc.checkpoint(args(0))
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    stream
      .foreachRDD { rdd =>
        rdd.mapPartitions { iterator =>

          val props = System.getProperties
          props.put("request.required.acks", "1")

          val producer = new KafkaProducer[String, Status](props)

          val iter = iterator.map { status =>
            val data = new ProducerRecord[String, Status](
              "twitter", status)
            producer.send(data)
            status
          }

          println(iter.size)

          producer.close(100L, TimeUnit.MILLISECONDS)

          Iterator.empty
        }
        .count()
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

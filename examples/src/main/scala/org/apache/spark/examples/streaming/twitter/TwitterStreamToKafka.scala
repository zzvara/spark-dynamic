
package org.apache.spark.examples.streaming.twitter

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterStreamToKafka {
  def main(args: Array[String]): Unit = {

    val cfg = new SparkConf().setJars(Seq(args(1), args(2)))

    val ssc = new StreamingContext(cfg, Seconds(2))
    ssc.checkpoint(args(0))
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    stream
      .foreachRDD { rdd =>
        rdd.mapPartitions { iterator =>

          val props = System.getProperties
          props.put("request.required.acks", "1")

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

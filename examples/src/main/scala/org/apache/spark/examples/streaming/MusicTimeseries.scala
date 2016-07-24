
package org.apache.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.AccumulatorParam.Weightable
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

object MusicTimeseries {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Streaming MusicTimeseries")
      .setJars(Seq(args(1)))
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams: Map[String, String] = Map(
      "group.id" -> args(0),
      "zookeeper.connect" -> "localhost:2181",
      "auto.offset.reset" -> "smallest"
    )
    val topics = Map("default" -> 1)

    val records =
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY
      )
      .map(pair => new MusicRecord(pair._2.split("""\|""")))
      .flatMap(record => record.tags.map(t => (t, (t, record))))

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


class MusicRecord(
  val time: Long,
  val userID: Int,
  val trackID: Int,
  val playtime: Int,
  val sessionID: Int,
  val playlists: Seq[Int],
  val tags: Seq[Int],
  val subsType: String,
  val artists: Seq[Int],
  val albums: Seq[Int])
extends Weightable with Serializable {
  def this(split: Array[String]) = {
    this(split(0).toLong,
      split(1).toInt,
      split(2).toInt,
      split(3).toInt,
      split(4).toInt,
      split(5).split(",").map(_.toInt),
      split(6).split(",").map(_.toInt),
      split(7),
      split(8).split(",").map(_.toInt),
      split(9).split(",").filter(!_.contains("None")).map(_.toInt))
  }

  override def complexity(): Int = tags.size
}

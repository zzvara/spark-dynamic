package org.apache.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable.SynchronizedQueue
import scala.io.Source

object RepartitioningTest {

  def main(args: Array[String]) {
    val filePath = "C:\\Users\\szape\\Work\\Projects\\ER-DR\\data\\timeseries_ordered.txt"
    val numPartitions = args(0).toInt //10
    val recordsPerMinibatch = args(1).toInt //10000
    val sleepTime = args(2).toLong //100

    val sparkConf = new SparkConf()
      .setAppName("RepartitioningTest")
      .setJars(Array[String](
        "C:\\Users\\szape\\Git\\dynamic-repartitioning-2.0\\assembly\\target\\scala-2.12\\jars" +
          "\\spark-core_2.12-3.0.0-SNAPSHOT.jar"))
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val rddQueue = new SynchronizedQueue[RDD[String]]()

    new Thread(new Runnable {
      override def run() = {
        Source.fromFile(filePath).getLines.grouped(recordsPerMinibatch).foreach(group => {
          while (rddQueue.length >= 2) {
            Thread.sleep(sleepTime)
          }
          rddQueue += ssc.sparkContext.parallelize(group, numPartitions)
        })
      }
    }).start()

    ssc.queueStream(rddQueue, oneAtATime = true)
      .map(line => {
        new MusicRecord(line.split("""\|"""))
      })
      .filter(x => !x.tags.contains(-1))
      .flatMap(record => record.tags.map(t => (t.toString, record.userID.toString)))
      .groupByKey(numPartitions)
      .print(10)

    //ssc.checkpoint("tmp/szape/RepartitioningTest")

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
  extends Serializable {
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

  override def toString: String = {
    s"(time: $time, userID: $userID, trackID: $trackID, playtime: $playtime, sessionID: $sessionID," +
      s" playlists: $playlists, tags: $tags, subsType: $subsType, artists: $artists, albums: $albums)"
  }
}
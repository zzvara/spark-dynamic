package org.apache.spark.examples.repartitioning

import org.apache.spark.AccumulatorParam.Weightable
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.SynchronizedQueue
import scala.io.Source

object QueueStreamMusicTimeSeries {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("QueueStreamMusicTimeseries")
      .setJars(Seq(args(1)))
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val rddQueue = new SynchronizedQueue[RDD[String]]()
    val recordsPerMinibatch = 10000
    val sleepTime = 100
    val numPartitions = args(2).toInt

    //    class FileReaderThread extends Runnable {
    //      override def run(): Unit = {
    //
    //        Source.fromFile(args(0)).getLines.grouped(linesInRDD).foreach(group => {
    //          rddQueue += ssc.sparkContext.parallelize(group, numPartitions)
    //          Thread.sleep(sleepTime)
    //        })
    //
    ////        try {
    ////          val br = new BufferedReader(new FileReader(""))
    ////          var line = br.readLine()
    ////          while({
    ////            val line = br.readLine()
    ////            line != null
    ////          }) {
    ////            line.
    ////          }
    ////        })
    ////        (1 until linesInRDD).
    //
    //      }
    //    }

    new Thread(new Runnable {
      override def run(): Unit = {
        Source.fromFile(args(0)).getLines.grouped(recordsPerMinibatch).foreach(group => {
          //              println(s"### queue length : ${rddQueue.length}")
          while (rddQueue.length >= 2) {
            Thread.sleep(sleepTime)
          }
          rddQueue += ssc.sparkContext.parallelize(group, numPartitions)
        })
      }
    }).start()

    val records = ssc.queueStream(rddQueue, oneAtATime = true)
      .map(line => {
        new MusicRecord(line.split("""\|"""))
      })
      .flatMap(record => record.tags.map(t => (t, (t, record))))
      .groupByKey(numPartitions)
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

  override def toString: String = {
    s"(time: $time, userID: $userID, trackID: $trackID, playtime: $playtime, sessionID: $sessionID," +
      s" playlists: $playlists, tags: $tags, subsType: $subsType, artists: $artists, albums: $albums)"
  }
}

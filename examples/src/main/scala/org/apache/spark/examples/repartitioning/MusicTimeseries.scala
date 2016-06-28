
package org.apache.spark.examples.repartitioning

import org.apache.spark.AccumulatorParam.Weightable
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object MusicTimeseries {
  def main(args: Array[String]) {
    val configuration = new SparkConf()
      .setAppName("MusicTimeseries")
      .setJars(Seq(args(1)))
    val context = new SparkContext(configuration)

    val records =
      context
        .textFile(args(0), args(2).toInt)
        .map(line => new Record(line.split("""\|""")))
        .flatMap(record => record.tags.map(t => (t, record)))
        //.filter(_._1 != -1)

    /*
    val frequencies =
      records
        .reduceByKey(_ + _)
        .sortBy(_._2, false)
        .take(50)
        */

    // frequencies foreach println

    records.groupByKey(new Partitioner {
      val hp: HashPartitioner = new HashPartitioner(399)
      override def numPartitions: Int = 400
      override def getPartition(key: Any): Int = {
        if (key.asInstanceOf[Int] == -1) {
          399
        } else {
          hp.getPartition(key)
        }
      }
    }).map {
      _._2.map {
        x => x.complexity()
      }
    }.count()

    Thread.sleep(60 * 60 * 1000)
  }
}


class Record(
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

/**
(-1,8472447)
(205245,5854849)
(115355,3885876)
(11056,3362907)
(70618,2863653)
(189631,2770475)
(81223,1810631)
(115752,1749493)
(11242,1675836)
(154891,1425819)
(46208,1285690)
(35060,1061220)
(84597,968750)
(54087,938175)
(3982,864571)
(11957,838152)
(218701,830506)
(117167,820202)
(103394,816365)
(43212,741328)
(107401,739571)
(226723,685157)
(76913,641352)
(4425,619299)
(115684,601585)
(122769,586406)
(6120,566953)
(195173,553426)
(227682,553212)
(70625,552880)
(3668,535122)
(83064,528012)
(105199,517672)
(193464,513575)
(198998,503399)
(107398,482506)
(144283,436511)
(24358,431379)
(3424,428986)
(1854,422952)
(170251,422681)
(195456,413239)
(237214,412752)
(194264,399706)
(444,396391)
(186445,380744)
(64978,367511)
(50604,364213)
(144192,360136)
(204710,359457)
  */
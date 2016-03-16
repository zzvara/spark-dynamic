
package org.apache.spark.examples.repartitioning

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object MurmurSpeedTest {
  def main(args: Array[String]): Unit = {
    var murmurTimes: mutable.ListBuffer[Long] = mutable.ListBuffer[Long]()
    var javaTimes: mutable.ListBuffer[Long] = mutable.ListBuffer[Long]()
    var t: Long = 0

    val numberOfMeasurements = 200

    for (i <- 1 to numberOfMeasurements) {
      t = System.nanoTime()

      runMurmur(1000000)

      murmurTimes += System.nanoTime() - t
    }

    println {
      (murmurTimes.sum / murmurTimes.size) / (1000 * 1000)
    }

    for (i <- 1 to numberOfMeasurements) {
      t = System.nanoTime()

      runScala(1000000)

      javaTimes += System.nanoTime() - t
    }

    println {
      (javaTimes.sum / javaTimes.size) / (1000 * 1000)
    }
  }

  def runMurmur(runs: Int): Unit = {
    var hashCode: Int = 0
    var dataPoint: String = ""

    for (i <- 1 to runs) {
      dataPoint = toString
      hashCode = MurmurHash3.stringHash(dataPoint)
    }
  }

  def runScala(runs: Int): Unit = {
    var hashCode: Int = 0
    var dataPoint: String = ""

    for (i <- 1 to runs) {
      dataPoint = toString
      hashCode = dataPoint.hashCode
    }
  }
}

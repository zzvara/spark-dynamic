package org.apache.spark.examples.repartitioning

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.Stream
import org.apache.spark.streaming.repartitioning.decider.NaivRetentiveStrategy
import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.language.reflectiveCalls

object PartitionerUpdateTest {
  def main(args: Array[String]): Unit = {
    test1()
  }

  def test1() = {
    val sparkConf = new SparkConf()
      .setAppName("QueueStreamMusicTimeseries")
      .setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val numPartitions = 4
    val numKeys = 8
    val minibatchSize = 40
    val numMiniBatches = 6
    val miniBatches = Array.ofDim[(Int, String)](numMiniBatches, numPartitions, minibatchSize / numPartitions)
    val keyHistograms = Array.fill[DataCharacteristicsAccumulator](numMiniBatches, numPartitions)(new DataCharacteristicsAccumulator)
    val stream = Stream(0, Time(15), Seconds(5))

    val strategy = new NaivRetentiveStrategy(0, stream, 1, Some(() => numPartitions)) {
      var latestPartitioner: Partitioner = new HashPartitioner(numPartitions)

      override protected def getNewPartitioner(partitioningInfo: PartitioningInfo): Partitioner = {
        latestPartitioner = super.getNewPartitioner(partitioningInfo)
        latestPartitioner
      }

      def getLatestPartitioningInfo: Option[PartitioningInfo] = {
        latestPartitioningInfo
      }

      override def repartition(): Boolean = {
        val doneRepartitioning = if (preDecide()) {
          val globalHistogram = computeGlobalHistogram
          if (decideAndValidate(globalHistogram)) {
            // we commented out resetPartitioners intentionally, because it is not part of the logic to be tested, but
            // it would require a running cluster and a full fledged streaming topology in the background
            // resetPartitioners(
            getNewPartitioner(getPartitioningInfo(globalHistogram))
            // )
            true
          } else {
            false
          }
        } else {
          false
        }
        cleanup()
        doneRepartitioning
      }
    }

    // uniform under hash partitioning (5, 5, 5, 5, 5, 5, 5, 5)
    miniBatches(0) = Array(
      Array((3, "v"), (6, "v"), (2, "v"), (1, "v"), (0, "v"), (6, "v"), (4, "v"), (7, "v"), (4, "v"), (0, "v")),
      Array((3, "v"), (5, "v"), (0, "v"), (4, "v"), (5, "v"), (6, "v"), (1, "v"), (7, "v"), (2, "v"), (2, "v")),
      Array((7, "v"), (6, "v"), (2, "v"), (1, "v"), (0, "v"), (5, "v"), (4, "v"), (1, "v"), (4, "v"), (3, "v")),
      Array((3, "v"), (5, "v"), (7, "v"), (6, "v"), (5, "v"), (3, "v"), (1, "v"), (0, "v"), (7, "v"), (2, "v")))
    // keys are skewed (14, 9, 6, 4, 3, 2, 1, 1) sCut = 2
    miniBatches(1) = Array(
      Array((0, "v"), (1, "v"), (2, "v"), (0, "v"), (3, "v"), (0, "v"), (4, "v"), (2, "v"), (1, "v"), (0, "v")),
      Array((3, "v"), (6, "v"), (1, "v"), (5, "v"), (0, "v"), (4, "v"), (0, "v"), (1, "v"), (0, "v"), (2, "v")),
      Array((1, "v"), (2, "v"), (0, "v"), (1, "v"), (0, "v"), (3, "v"), (0, "v"), (1, "v"), (0, "v"), (4, "v")),
      Array((2, "v"), (5, "v"), (1, "v"), (0, "v"), (3, "v"), (0, "v"), (1, "v"), (7, "v"), (2, "v"), (0, "v")))
    // insignificant change (13, 8, 8, 4, 3, 2, 1, 1)
    miniBatches(2) = Array(
      Array((2, "v"), (1, "v"), (2, "v"), (0, "v"), (3, "v"), (0, "v"), (4, "v"), (2, "v"), (1, "v"), (0, "v")),
      Array((3, "v"), (6, "v"), (1, "v"), (5, "v"), (0, "v"), (4, "v"), (0, "v"), (1, "v"), (0, "v"), (2, "v")),
      Array((1, "v"), (2, "v"), (0, "v"), (1, "v"), (0, "v"), (3, "v"), (0, "v"), (2, "v"), (0, "v"), (4, "v")),
      Array((2, "v"), (5, "v"), (1, "v"), (0, "v"), (3, "v"), (0, "v"), (1, "v"), (7, "v"), (2, "v"), (0, "v")))
    // key 1 grows over key 0 (8, 12, 9, 4, 3, 2, 1, 1)
    miniBatches(3) = Array(
      Array((2, "v"), (1, "v"), (2, "v"), (0, "v"), (3, "v"), (1, "v"), (4, "v"), (2, "v"), (1, "v"), (0, "v")),
      Array((3, "v"), (6, "v"), (1, "v"), (5, "v"), (0, "v"), (4, "v"), (1, "v"), (2, "v"), (0, "v"), (2, "v")),
      Array((1, "v"), (2, "v"), (1, "v"), (1, "v"), (0, "v"), (3, "v"), (0, "v"), (2, "v"), (1, "v"), (4, "v")),
      Array((2, "v"), (5, "v"), (1, "v"), (0, "v"), (3, "v"), (1, "v"), (1, "v"), (7, "v"), (2, "v"), (0, "v")))
    // key 2 becomes the largest key, need to repartition now (6, 8, 15, 4, 3, 2, 1, 1) sCut = 1
    miniBatches(4) = Array(
      Array((2, "v"), (1, "v"), (2, "v"), (0, "v"), (3, "v"), (2, "v"), (4, "v"), (2, "v"), (1, "v"), (0, "v")),
      Array((3, "v"), (6, "v"), (2, "v"), (5, "v"), (2, "v"), (4, "v"), (1, "v"), (2, "v"), (0, "v"), (2, "v")),
      Array((1, "v"), (2, "v"), (1, "v"), (2, "v"), (2, "v"), (3, "v"), (0, "v"), (2, "v"), (1, "v"), (4, "v")),
      Array((2, "v"), (5, "v"), (1, "v"), (0, "v"), (3, "v"), (2, "v"), (1, "v"), (7, "v"), (2, "v"), (0, "v")))
    // distribution smoothens again, repartitioning is needed (5, 5, 5, 5, 5, 5, 5, 5)
    // TODO check if we can switch back to hash partitioning (add different distance-from-uniform treshold)
    miniBatches(5) = Array(
      Array((3, "v"), (6, "v"), (2, "v"), (1, "v"), (0, "v"), (6, "v"), (4, "v"), (7, "v"), (4, "v"), (0, "v")),
      Array((3, "v"), (5, "v"), (0, "v"), (4, "v"), (5, "v"), (6, "v"), (1, "v"), (7, "v"), (2, "v"), (2, "v")),
      Array((7, "v"), (6, "v"), (2, "v"), (1, "v"), (0, "v"), (5, "v"), (4, "v"), (1, "v"), (4, "v"), (3, "v")),
      Array((3, "v"), (5, "v"), (7, "v"), (6, "v"), (5, "v"), (3, "v"), (1, "v"), (0, "v"), (7, "v"), (2, "v")))

    (0 until numMiniBatches).foreach(i1 => (0 until numPartitions).foreach( i2 => {
      val keyHistogram = keyHistograms(i1)(i2)
      miniBatches(i1)(i2).foreach(r => keyHistogram.add((r._1, 1.0d)))
    }))

    def getPartitionHistogram(miniBatch: Array[Array[(Int, String)]]): scala.collection.mutable.HashMap[Int, Long] = {
      val partitioner = strategy.latestPartitioner
      val partitionHistogram = scala.collection.mutable.HashMap[Int, Long]()
      miniBatch.foreach(_.foreach(r => {
        val key = partitioner.getPartition(r._1)
        partitionHistogram.get(key) match {
          case Some(value) => partitionHistogram.update(key, value + 1L)
          case None => partitionHistogram.update(key, 1L)
        }
      }))
      partitionHistogram
    }

    strategy.onHistogramArrival(0, keyHistograms(0)(0))
    strategy.onHistogramArrival(1, keyHistograms(0)(1))
    strategy.onHistogramArrival(2, keyHistograms(0)(2))
    strategy.onHistogramArrival(3, keyHistograms(0)(3))
    var partitionHistogram = getPartitionHistogram(miniBatches(0))
    strategy.onPartitionMetricsArrival(0, partitionHistogram(0))
    strategy.onPartitionMetricsArrival(1, partitionHistogram(1))
    strategy.onPartitionMetricsArrival(2, partitionHistogram(2))
    strategy.onPartitionMetricsArrival(3, partitionHistogram(3))
    var didRepartition = strategy.repartition()
    assert(!didRepartition)
//    println(s"#$didRepartition")
//    println(s"#${strategy.getLatestPartitioningInfo}")

    strategy.onHistogramArrival(0, keyHistograms(1)(0))
    strategy.onHistogramArrival(1, keyHistograms(1)(1))
    strategy.onHistogramArrival(2, keyHistograms(1)(2))
    strategy.onHistogramArrival(3, keyHistograms(1)(3))
    partitionHistogram = getPartitionHistogram(miniBatches(1))
    strategy.onPartitionMetricsArrival(0, partitionHistogram(0))
    strategy.onPartitionMetricsArrival(1, partitionHistogram(1))
    strategy.onPartitionMetricsArrival(2, partitionHistogram(2))
    strategy.onPartitionMetricsArrival(3, partitionHistogram(3))
    didRepartition = strategy.repartition()
    assert(didRepartition)

    strategy.onHistogramArrival(0, keyHistograms(2)(0))
    strategy.onHistogramArrival(1, keyHistograms(2)(1))
    strategy.onHistogramArrival(2, keyHistograms(2)(2))
    strategy.onHistogramArrival(3, keyHistograms(2)(3))
    partitionHistogram = getPartitionHistogram(miniBatches(2))
    strategy.onPartitionMetricsArrival(0, partitionHistogram(0))
    strategy.onPartitionMetricsArrival(1, partitionHistogram(1))
    strategy.onPartitionMetricsArrival(2, partitionHistogram(2))
    strategy.onPartitionMetricsArrival(3, partitionHistogram(3))
    didRepartition = strategy.repartition()
    assert(!didRepartition)

    strategy.onHistogramArrival(0, keyHistograms(3)(0))
    strategy.onHistogramArrival(1, keyHistograms(3)(1))
    strategy.onHistogramArrival(2, keyHistograms(3)(2))
    strategy.onHistogramArrival(3, keyHistograms(3)(3))
    partitionHistogram = getPartitionHistogram(miniBatches(3))
    strategy.onPartitionMetricsArrival(0, partitionHistogram(0))
    strategy.onPartitionMetricsArrival(1, partitionHistogram(1))
    strategy.onPartitionMetricsArrival(2, partitionHistogram(2))
    strategy.onPartitionMetricsArrival(3, partitionHistogram(3))
    didRepartition = strategy.repartition()
    assert(!didRepartition)

    strategy.onHistogramArrival(0, keyHistograms(4)(0))
    strategy.onHistogramArrival(1, keyHistograms(4)(1))
    strategy.onHistogramArrival(2, keyHistograms(4)(2))
    strategy.onHistogramArrival(3, keyHistograms(4)(3))
    partitionHistogram = getPartitionHistogram(miniBatches(4))
    strategy.onPartitionMetricsArrival(0, partitionHistogram(0))
    strategy.onPartitionMetricsArrival(1, partitionHistogram(1))
    strategy.onPartitionMetricsArrival(2, partitionHistogram(2))
    strategy.onPartitionMetricsArrival(3, partitionHistogram(3))
    didRepartition = strategy.repartition()
    assert(didRepartition)

    strategy.onHistogramArrival(0, keyHistograms(5)(0))
    strategy.onHistogramArrival(1, keyHistograms(5)(1))
    strategy.onHistogramArrival(2, keyHistograms(5)(2))
    strategy.onHistogramArrival(3, keyHistograms(5)(3))
    partitionHistogram = getPartitionHistogram(miniBatches(5))
    strategy.onPartitionMetricsArrival(0, partitionHistogram(0))
    strategy.onPartitionMetricsArrival(1, partitionHistogram(1))
    strategy.onPartitionMetricsArrival(2, partitionHistogram(2))
    strategy.onPartitionMetricsArrival(3, partitionHistogram(3))
    didRepartition = strategy.repartition()
    assert(didRepartition)
  }
}
package org.apache.spark.streaming.repartitioning.decider

import org.apache.spark.repartitioning.Decider
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.{ShuffledDStream, Stream}
import org.apache.spark.streaming.repartitioning.StreamingUtils
import org.apache.spark.util.DataCharacteristicsAccumulator
import org.apache.spark.{Partitioner, PartitioningInfo, SparkEnv, SparkException}

import scala.collection.mutable

/**
  * A simple strategy to decide when and how to repartition a stage.
  */
class NaivRetentiveStrategy(
  streamID: Int,
  stream: Stream,
  perBatchSamplingRate: Int = 1,
  resourceStateHandler: Option[() => Int] = None)
extends StreamingDecider(streamID, stream, perBatchSamplingRate, resourceStateHandler) {
  protected val partitionerHistory = scala.collection.mutable.Seq[Partitioner]()
  protected val partitionHistogram = mutable.HashMap[Int, Long]()
  protected var retentiveKeyHistogram: Option[scala.collection.Seq[(Any, Double)]] = None
  protected var retentivePartitionHistogram: Option[scala.collection.Seq[Double]] = None

  protected val histogramComparisionThreshold =
    SparkEnv.get.conf.getDouble("spark.repartitioning.histogram.comparision-threshold", 0.01d)
  protected val retentiveKeyHistogramWeight =
    SparkEnv.get.conf.getDouble("spark.repartitioning.streaming.retentive-key.weight", 0.8d)
  protected val retentivePartitionHistogramWeight =
    SparkEnv.get.conf.getDouble("spark.repartitioning.streaming.retentive-partition.weight", 0.9d)

  resourceStateHandler.foreach { handler =>
    numberOfPartitions = handler.apply()
    logInfo(s"Updating number of partitions with the resource-state handler to" +
      s"$numberOfPartitions.")
  }

  /**
    * Called by the RepartitioningTrackerMaster if new histogram arrives
    * for this particular job's strategy.
    */
  override def onHistogramArrival(partitionID: Int,
                                  keyHistogram: DataCharacteristicsAccumulator): Unit = {
    this.synchronized {
      logInfo(s"Recording histogram arrival for partition $partitionID.",
        "DRCommunication", "DRHistogram")
      histograms.update(partitionID, keyHistogram)
    }
  }

  def onPartitionMetricsArrival(partitionID: Int, recordsRead: Long): Unit = {
    this.synchronized {
      logInfo(s"Recording metrics for partition $partitionID.",
        "DRCommunication", "DRHistogram")
      partitionHistogram.update(partitionID, recordsRead)
    }
  }

  override protected def clearHistograms(): Unit = {
    histograms.clear()
    partitionHistogram.clear()
  }

  override protected def getGlobalHistogram = {
    val globalHistogram = currentGlobalHistogram.getOrElse(computeGlobalHistogram)
    retentiveKeyHistogram match {
      case Some(histogram) =>
        logInfo(s"Getting histogram by calculating the retentive histogram with" +
          s" a retentive weight of $retentiveKeyHistogramWeight.")
        retentiveKeyHistogram = Some(DataCharacteristicsAccumulator.weightedMerge(
          0.0d, retentiveKeyHistogramWeight)(
          histogram.toMap, globalHistogram
        ).sortBy(-_._2).take(totalSlots))
      case None =>
        retentiveKeyHistogram = Some(globalHistogram)
    }
    logInfo(
      retentiveKeyHistogram.get.foldLeft(
        s"Retentive histogram is:\n")((x, y) =>
        x + s"\t${y._1}\t->\t${y._2}\n"), "DRHistogram")
    logObject(("retentiveHistogram", streamID, retentiveKeyHistogram.get))
    retentiveKeyHistogram.get
  }

  /**
    * @todo Check distance from uniform, fall back to HashPartitioning if close.
    */
  private def isSignificantChange(partitioningInfo: Option[PartitioningInfo],
                                  partitionHistogram: Seq[Double],
                                  threshold: Double): Boolean = {
    val maxPartition = partitionHistogram.max
    val minPartition = partitionHistogram.min
    logInfo(s"Difference between maximum and minimum of partition histogram is " +
      s"$maxPartition - $minPartition = ${maxPartition-minPartition}")
    logInfo(s"Relative size of the maximal partition to the ideal average is " +
      s"${(maxPartition / partitionHistogram.sum) / (1.0d / numberOfPartitions)}")
    logObject(("partitionHistogram", streamID, partitionHistogram))

    if (SparkEnv.get.conf.getBoolean("spark.repartitioning.significant-change.backdoor",
      defaultValue = false)) {
      return false
    }

    if (SparkEnv.get.conf.getBoolean("spark.repartitioning.significant-change.always-yes",
      defaultValue = false)) {
      return true
    }

    val sCut = partitioningInfo.map(_.sCut).getOrElse(0)

    val maxInSCut: Double = if (sCut == 0) {
      1.0d / numberOfPartitions
    } else {
      partitionHistogram.take(sCut).max
    }
    val maxOutsideSCut: Double = partitionHistogram.drop(sCut).max
    val isSignificantChange = maxInSCut + threshold < maxOutsideSCut
    if (isSignificantChange) {
      logInfo("Significant change detected.")
    } else {
      logInfo("Significant changed not detected.")
    }
    isSignificantChange
  }

  /**
    * Decides whether repartitioning is needed based on:
    * - the partition histogram of the current stage,
    * - the Partitioner of the previous stage.
    *
    * Note that no global histogram is used in this point.
    */
  override protected def preDecide(): Boolean = {
    logInfo(s"Deciding if need any repartitioning now for stream " +
      s"with ID $streamID.", "DRRepartitioner")
    logInfo(s"Number of received histograms: ${histograms.size}", "DRHistogram")
    if (histograms.size < SparkEnv.get.conf.getInt("spark.repartitioning.histogram-threshold", 2)) {
      logWarning("Histogram threshold is not met.")
      return false
    }

    val retentivePartitionHistogram = computeRetentivePartitionHistogram

    if(retentivePartitionHistogram.sum > 0) {
      isSignificantChange(
        latestPartitioningInfo,
        retentivePartitionHistogram,
        histogramComparisionThreshold)
    } else {
      true
    }
  }

  private def computeRetentivePartitionHistogram: Seq[Double] = {
    val rawPartitionHistogram =
      partitionHistogram.toSeq.sortBy(_._1).map(_._2).padTo(numberOfPartitions, 0L)
    val sum = rawPartitionHistogram.sum
    val normalizedPartitionHistogram = rawPartitionHistogram.map(_.toDouble / sum)

    retentivePartitionHistogram match {
      case Some(retentiveHistogram) if (retentiveHistogram.size
        == normalizedPartitionHistogram.size) =>
        retentivePartitionHistogram =
          Some(retentiveHistogram.zip(normalizedPartitionHistogram).map {
            case (a, b) =>
              (a * retentivePartitionHistogramWeight) +
                (b * (1 - retentivePartitionHistogramWeight))
          })
      case _ =>
        logInfo("Resetting retentive partition histogram.")
        retentivePartitionHistogram = Some(normalizedPartitionHistogram)
    }

    logInfo(s"Computed retentive partition histogram." +
      s"Size of the first element is: ${retentivePartitionHistogram.get.head}.")

    retentivePartitionHistogram.get
  }

  override protected def decideAndValidate(
                                            globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    isValidHistogram(globalHistogram)
  }

  /**
    * Sends the strategy to each worker.
    * It asks the RepartitioningTrackerMaster to broadcast the new
    * strategy to workers.
    *
    * (Search for a ShuffledDStream. This is the
    * only DStream that has a partitioner! Partitioner is used for the underlying
    * RDD. Partitioner should be changed!)
    */
  override protected def resetPartitioners(newPartitioner: Partitioner): Unit = {
    StreamingUtils.getChildren(streamID) match {
      case head :: tail =>
        head match {
          case shuffledDStream: ShuffledDStream[_, _, _] =>
            logInfo(s"Resetting partitioner for DStream with ID $streamID to partitioner " +
              s" ${newPartitioner.toString}.")
            logObject(("partitionerReset", streamID, newPartitioner))
            shuffledDStream.partitioner = newPartitioner
            partitionerHistory :+ newPartitioner
          case _ =>
            throw new SparkException("Not a ShuffledDStream! Sorry.")
        }
      case Nil =>
        throw new SparkException("DStream not found in DStreamGraph!")
    }
  }

  override protected def cleanup(): Unit = {
    super.cleanup()
    clearHistograms()
  }
}

object NaivRetentiveStrategy extends StreamingDeciderFactory {
  override def apply(streamID: Int,
                     stream: Stream,
                     perBatchSamplingRate: Int,
                     resourceStateHandler: Option[() => Int]): StreamingDecider = {
    new NaivRetentiveStrategy(streamID, stream, perBatchSamplingRate, resourceStateHandler)
  }
}
package org.apache.spark.streaming.repartitioning.core

import org.apache.spark.internal.Logging
import org.apache.spark.repartitioning.core.Configuration
import org.apache.spark.streaming.dstream.Stream
import org.apache.spark.streaming.repartitioning.decider.{NaivRetentiveStrategy, StreamingDeciderFactory}
import org.apache.spark.util.{DataCharacteristicsAccumulator, Utils}

import scala.collection.mutable

trait StreamingRepartitioningTrackerMasterHelper extends Logging {
  protected val _streamData = mutable.HashMap[Int, MasterStreamData]()

  protected var isInitialized = false

  /**
    * Please not that here we are importing a companion object, marked with sign `$`.
    */
  protected val deciderFactoryClass =
    Utils.classForName(
      Configuration.internal().getString("repartitioning.streaming.decider.factory") + "$"
    ).asInstanceOf[Class[StreamingDeciderFactory]]

  logInfo(s"The decider factory class is [${deciderFactoryClass.getClass.getName}].")

  protected val deciderFactory = deciderFactoryClass
      .getField("MODULE$")
      .get(deciderFactoryClass)
      .asInstanceOf[StreamingDeciderFactory]

  def getTotalSlots: Int

  protected def getNumberOfPartitions(streamID: Int): Int

  def updateLocalHistogramForStreaming(
    stream: Stream,
    taskID: Long,
    partitionID: Int,
    dataCharacteristics: DataCharacteristicsAccumulator
  ): Unit = {
    _streamData.find { _._2.hasParent(stream.ID) } match {
      case Some((sID, streamData)) =>
        logInfo(s"Updating local histogram for task $taskID " +
                s"in stream ${stream.ID} with output stream ID $sID.")
        streamData.strategies.getOrElseUpdate(
          stream.ID,
          deciderFactory(stream.ID, stream, 1, Some(() => getTotalSlots))
        ).onHistogramArrival(partitionID, dataCharacteristics)
      case None => logWarning(
          s"Could not update local histogram for streaming," +
          s" since streaming data does not exist for DStream" +
          s" ID ${stream.ID}!")
    }
  }

  protected def updatePartitionMetrics(
    stream: Stream,
    taskID: Long,
    partitionID: Int,
    recordsRead: Long
  ): Unit = {
    _streamData.find {
      _._2.hasParent(stream.ID)
    } match {
      case Some((sID, streamData)) =>
        logInfo(s"Updating partition metrics for task $taskID " +
                s"in stream ${stream.ID}.")
        val id = stream.ID
        streamData.strategies.getOrElseUpdate(
          id,
          deciderFactory(stream.ID, stream, getTotalSlots)
        ).asInstanceOf[NaivRetentiveStrategy].onPartitionMetricsArrival(partitionID, recordsRead)
      case None => logWarning(
        s"Could not update local histogram for streaming," +
        s" since streaming data does not exist for DStream" +
        s" ID ${stream.ID}!")
    }
  }
}


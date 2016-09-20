package org.apache.spark.streaming.repartitioning.core

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.repartitioning.RepartitioningStageData
import org.apache.spark.repartitioning.core.Scanner
import org.apache.spark.repartitioning.core.messaging.{ScanStrategies, StandaloneStrategy}
import org.apache.spark.streaming.repartitioning.{RepartitioningStreamData, StreamingScanStrategy}

import scala.collection.mutable

trait StreamingRepartitioningTrackerWorkerHelper extends Logging {
  protected val streamData = mutable.HashMap[Int, RepartitioningStreamData]()

  protected def getStageData: mutable.HashMap[Int, RepartitioningStageData[TaskContext, TaskMetrics]]

  protected def privateReceive: PartialFunction[Any, Unit] = {
    case ScanStrategies(scanStrategies) =>
      logInfo(s"Received a list of scan strategies, with size of ${scanStrategies.length}.")
      scanStrategies.foreach {
        /**
          * @todo The standalone part should only be in the default tracker.
          */
        case StandaloneStrategy(stageID, scanner) =>
          getStageData.update(stageID, RepartitioningStageData[TaskContext, TaskMetrics](
            scanner.asInstanceOf[Scanner[TaskContext, TaskMetrics]]))
        case StreamingScanStrategy(streamID, strategy, parentStreams) =>
          logInfo(s"Received streaming strategy for stream ID $streamID.")
          streamData.update(streamID, RepartitioningStreamData(streamID, strategy, parentStreams))
      }
    case StreamingScanStrategy(streamID, strategy, parentStreams) =>
      logInfo(s"Received streaming strategy for stream ID $streamID.")
      streamData.update(streamID, RepartitioningStreamData(streamID, strategy, parentStreams))
  }
}

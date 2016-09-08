package org.apache.spark.streaming.repartitioning

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.repartitioning.{RepartitioningStageData, RepartitioningTrackerWorker, ScanStrategies, StandaloneStrategy}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.streaming.dstream.Stream

import scala.collection.mutable

private[spark] class StreamingRepartitioningTrackerWorker(
  override val rpcEnv: RpcEnv,
  conf: SparkConf,
  executorId: String)
extends RepartitioningTrackerWorker(rpcEnv, conf, executorId) {
  private val streamData = mutable.HashMap[Int, RepartitioningStreamData]()

  override def receive: PartialFunction[Any, Unit] = {
    privateReceive orElse super.receive
  }

  private def privateReceive: PartialFunction[Any, Unit] = {
    case ScanStrategies(scanStrategies) =>
      logInfo(s"Received a list of scan strategies, with size of ${scanStrategies.length}.")
      scanStrategies.foreach {
        /**
          * @todo The standalone part should only be in the default tracker.
          */
        case StandaloneStrategy(stageID, scanner) =>
          stageData.update(stageID, RepartitioningStageData(scanner))
        case StreamingScanStrategy(streamID, strategy, parentStreams) =>
          logInfo(s"Received streaming strategy for stream ID $streamID.", "DRCommunication")
          streamData.update(streamID, RepartitioningStreamData(streamID, strategy, parentStreams))
      }
    case StreamingScanStrategy(streamID, strategy, parentStreams) =>
      logInfo(s"Received streaming strategy for stream ID $streamID.", "DRCommunication")
      streamData.update(streamID, RepartitioningStreamData(streamID, strategy, parentStreams))
  }

  override def isDataAware(rdd: RDD[_]): Boolean = {
    if (rdd.getProperties.contains("stream")) {
      val streamProperty = rdd.getProperties("stream").asInstanceOf[Stream]
      streamData.find(_._2.parentStreams.contains(streamProperty.ID)).map(_._2) match {
        case Some(data) =>
          val remaining = (streamProperty.time - data.strategy.zeroTime).milliseconds %
            (streamProperty.batchDuration.milliseconds * data.strategy.perBatchSamplingRate)
          val isDataAwareForTime = remaining == 0
          if (isDataAwareForTime) {
            logInfo(s"RDD is data-aware to time ${streamProperty.time.milliseconds} and stream " +
              s"${streamProperty.ID}.")
          }
          isDataAwareForTime
        case None =>
          logWarning(s"RDD with property 'stream' has no stream data associated!")
          false
      }
    } else {
      logInfo("RDD not with property 'stream', falling back to base tracker for decision.")
      super.isDataAware(rdd)
    }
  }
}

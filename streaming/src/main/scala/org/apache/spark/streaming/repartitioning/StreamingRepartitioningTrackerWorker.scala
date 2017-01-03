package org.apache.spark.streaming.repartitioning

import hu.sztaki.drc.component.StreamingRepartitioningTrackerWorkerHelper
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.repartitioning.RepartitioningTrackerWorker
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.streaming.dstream.Stream
import org.apache.spark.streaming.repartitioning.decider.StreamingDecider

private[spark] class StreamingRepartitioningTrackerWorker(
  override val rpcEnv: RpcEnv,
  conf: SparkConf,
  executorId: String)
extends RepartitioningTrackerWorker(rpcEnv, conf, executorId)
with StreamingRepartitioningTrackerWorkerHelper[TaskContext, TaskMetrics] {

  override def componentReceive: PartialFunction[Any, Unit] = {
    privateReceive orElse super.componentReceive
  }

  override protected def getStageData = stageData

  override def isDataAware(rdd: RDD[_]): Boolean = {
    if (rdd.getProperties.contains("stream")) {
      val streamProperty = rdd.getProperties("stream").asInstanceOf[Stream]
      streamData.find(_._2.parentStreams.contains(streamProperty.ID)).map(_._2) match {
        case Some(data) =>
          val remaining = (streamProperty.time - data.strategy.asInstanceOf[StreamingDecider].zeroTime).milliseconds %
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

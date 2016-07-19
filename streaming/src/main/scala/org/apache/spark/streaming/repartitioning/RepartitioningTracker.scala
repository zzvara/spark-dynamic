
package org.apache.spark.streaming.repartitioning

import org.apache.spark.AccumulatorParam.DataCharacteristicsAccumulatorParam
import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd, TaskInfo}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ShuffledDStream}

import scala.collection.mutable
import scala.collection.mutable.{Map, Set}


case class MasterStreamData(
    streamID: Int,
    relatedJobs: Set[Int] = Set[Int](),
    parentDStreams: scala.collection.immutable.Set[Int]
      = scala.collection.immutable.Set[Int]()
) {
  val strategies: Map[Int, Decider] =
    Map[Int, Decider]()

  def addJob(jobID: Int): MasterStreamData = {
    relatedJobs += jobID
    this
  }

  def hasParent(stream: Int): Boolean = {
    parentDStreams.contains(stream)
  }
}

private[spark] class StreamingRepartitioningTrackerMaster(
  override val rpcEnv: RpcEnv,
  conf: SparkConf)
extends RepartitioningTrackerMaster(rpcEnv, conf) {

  private val _streamData = mutable.HashMap[Int, MasterStreamData]()

  private val _jobData = mutable.HashMap[Int, MasterJobData]()

  /**
    * For streaming mini-batches only.
    *
    * @todo This is where the decision should be!
    * @hint Use the DStreamGraph from StreamingContext to look up the DStream
    *       and change the partitioner. (Search for a ShuffledDStream. This is the
    *       only DStream that has a partitioner! Partitioner is used for the underlying
    *       RDD. Partitioner should be changed!) That's all folks!
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logInfo("Job end detected!")
    _jobData.get(jobEnd.jobId).foreach { masterJobData =>
      logInfo("Job finished is a part of a streaming job.")

      val streamData = _streamData.getOrElse(masterJobData.streamID,
        throw new SparkException("Streaming data is malformed!"))

      streamData.strategies.foreach {
        _._2.decide()
      }
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    def getDependenciesReqursively(streams: scala.collection.immutable.Set[DStream[_]]):
      scala.collection.immutable.Set[DStream[_]] =
    {
      if (streams.isEmpty) {
        scala.collection.immutable.Set[DStream[_]]()
      } else {
        streams ++ getDependenciesReqursively(streams.flatMap(_.dependencies))
      }
    }

    val streamProperties = Some(jobStart.jobProperties).filter {
      properties =>
        properties.contains("type") &&
          properties.get("type").get.asInstanceOf[String] == "streaming"
    }
    if (streamProperties.isDefined) {
      val streamID = streamProperties.get
        .getOrElse("dstream_id",
          throw new SparkException("Job type is streaming, but DStream ID is missing."))
        .asInstanceOf[Int]
      logInfo(s"Job detected as a mini-batch for DStream with ID $streamID.")
      if (!_streamData.contains(streamID)) {
        logInfo(s"Registering stream with DStream ID $streamID.")

        val parentStreams =
          getDependenciesReqursively(
            StreamingContext.getActive().get.graph.getOutputStreams().toSet
          ).map(_.id)

        _streamData.update(streamID,
          new MasterStreamData(streamID, Set[Int](jobStart.jobId), parentStreams))
        _jobData.update(jobStart.jobId,
          new MasterJobData(jobStart.jobId, streamID))

        val scanStrategy = new StreamingStrategy(streamID)
        workers.values.foreach(_.reference.send(scanStrategy))
      } else {
        _streamData.update(streamID, {
          _streamData.get(streamID).get.addJob(jobStart.jobId)
        })
      }
    } else {
      logInfo(s"Regular job start detected, with ID ${jobStart.jobId}. " +
        s"Doing nothing special.")
    }
  }

  def updateLocalHistogramForStreaming(
    streamID: Int,
    taskInfo: TaskInfo,
    dataCharacteristics: DataCharacteristics[Any]
  ): Unit = {
    _streamData.find { _._2.hasParent(streamID) } match {
      case Some((sID, streamData)) =>
        logInfo("Updating local histogram.")
        streamData.strategies.getOrElseUpdate(
          streamID, new StreamingStrategy(streamID)
        ).onHistogramArrival(taskInfo.index,
          dataCharacteristics.toInfo(None, Some(dataCharacteristics.value),
            Some(dataCharacteristics.param)))
      case None => logWarning(
        s"Could not update local histogram for streaming," +
        s" since streaming data does not exist for DStream" +
        s" ID $streamID!")
    }
  }

  /**
    * The case when the task was actually a reoccurring part of a stream
    * processing pipeline. If that so, the task's stage properties hold
    * the ID of the DStream that belongs to the RDD that the task computes.
    *
    * 1) By this, we can identify tasks that belong to the same stage in a
    * stream processing. The main problem is that the stage and RDD IDs
    * are regenerated during the execution. From a streaming standpoint,
    * only the DStream ID is stable.
    *
    * There are no scanners initiated for a DStream task, so there's
    * nothing to do, only to record a retrieved DataCharacteristics
    * if there is one.
    *
    * 2) Task is part of a regular batch computation and is not related
    * to DStreams.
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.synchronized {
      taskEnd.taskInfo.stageProperties.flatMap(_.get("dstream_id")) match {
        case Some(dstreamID) =>
          logInfo(s"Task ended with DStream ID of $dstreamID.")
          taskEnd.taskMetrics.shuffleWriteMetrics match {
            case Some(shuffleWriteMetrics) =>
              val size = shuffleWriteMetrics.dataCharacteristics.value.size
              logInfo(s"DataCharacteristics size is $size.")
              updateLocalHistogramForStreaming(
                dstreamID.asInstanceOf[Int],
                taskEnd.taskInfo,
                shuffleWriteMetrics.dataCharacteristics)
            case None =>
              logWarning(s"No ShuffleWriteMetrics for task ${taskEnd.taskInfo.id}.")
          }
        case None =>
          super.onTaskEnd(taskEnd)
      }
    }
  }
}


/**
  * A simple strategy to decide when and how to repartition a stage.
  */
class StreamingStrategy(streamID: Int)
  extends Decider(streamID)  {

  protected var numPartitions: Int = 0
  private var minScale = 1.0d
  private val sCutHint = 0
  private val pCutHint = Math.pow(2, treeDepthHint - 1).toInt

  logInfo("sCutHint: " + sCutHint + ", pCutHint: " + pCutHint, "strongYellow")

  /**
    * Called by the RepartitioningTrackerMaster if new histogram arrives
    * for this particular job's strategy.
    */
  override def onHistogramArrival(partitionID: Int,
                                  keyHistogram: DataCharacteristicsInfo): Unit = {
    this.synchronized {
      val histogramMeta = keyHistogram.param.get.asInstanceOf[DataCharacteristicsAccumulatorParam]
      logInfo(s"Recording histogram arrival for partition $partitionID.",
              "DRCommunication", "DRHistogram")
      histograms.update(partitionID, keyHistogram)
    }
  }

  /**
    * Decides if repartitioning is needed. If so, constructs a new
    * partitioning function and sends the strategy to each worker.
    * It asks the RepartitioningTrackerMaster to broadcast the new
    * strategy to workers.
    */
  override def decide(): Boolean = {
    def lookupStreamChildrenReqursively(id: Int, streams: Array[DStream[_]]): Option[DStream[_]] = {
      if (streams.isEmpty) {
        None
      } else {
        streams.find(stream => stream.dependencies.exists(_.id == id)) match {
          case Some(dstream) => Some(dstream)
          case None =>
            lookupStreamChildrenReqursively(id, streams.flatMap(_.dependencies))
        }
      }
    }
    logInfo(s"Deciding if need any repartitioning now.", "DRRepartitioner")

    logInfo(s"Number of received histograms: ${histograms.size}", "strongCyan")
    val numRecords =
      histograms.values.map(
        _.param.get.asInstanceOf[DataCharacteristicsAccumulatorParam].recordsPassed).sum

    val globalHistogram =
      histograms.values.map(h => h.param.get.asInstanceOf[DataCharacteristicsAccumulatorParam]
        .normalize(h.value.get
          .asInstanceOf[scala.collection.immutable.Map[Any, Double]], numRecords))
        .reduce(DataCharacteristicsAccumulatorParam.merge[Any, Double](0.0)(
          (a: Double, b: Double) => a + b)
        )
        .toSeq.sortBy(-_._2)

    numPartitions = histograms.size

    val repartitioner = repartition(globalHistogram.take(histograms.size))

    lookupStreamChildrenReqursively(streamID,
      StreamingContext.getActive().get.graph.getOutputStreams()) match {
      case Some(dstream) =>
        dstream match {
          case shuffledDStream: ShuffledDStream[_, _, _] =>
            logInfo(s"Resetting partitioner for DStream with ID $streamID.")
            shuffledDStream.partitioner = repartitioner
          case _ =>
            throw new SparkException("Not a ShuffledDStream! Sorry.")
        }
      case None =>
        throw new SparkException("DStream not found in DStreamGraph!")
    }

    true
  }
}

private[spark] class StreamingRepartitioningTrackerWorker(
  override val rpcEnv: RpcEnv,
  conf: SparkConf,
  executorId: String)
extends RepartitioningTrackerWorker(rpcEnv, conf, executorId) {

}

class StreamingRepartitioningTrackerFactory extends RepartitioningTrackerFactory {
  def createMaster(rpcEnv: RpcEnv, conf: SparkConf): RepartitioningTrackerMaster = {
    new StreamingRepartitioningTrackerMaster(rpcEnv, conf)
  }
  def createWorker(rpcEnv: RpcEnv, conf: SparkConf,
                   executorId: String): RepartitioningTrackerWorker = {
    new StreamingRepartitioningTrackerWorker(rpcEnv, conf, executorId)
  }
}

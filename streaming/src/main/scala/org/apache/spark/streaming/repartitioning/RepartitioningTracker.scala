
package org.apache.spark.streaming.repartitioning

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, ShuffledDStream, Stream}
import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.collection.mutable
import scala.collection.mutable.{Map, Set}


/**
  * Scan strategy message sent to workers.
  */
private[spark] case class StreamingScanStrategy(
   streamID: Int,
   strategy: StreamingStrategy,
   parentStreams: collection.immutable.Set[Int])
extends ScanStrategy

/**
  *
  * @param streamID Stream is identified by the output DStream ID.
  * @param relatedJobs Mini-batches which has been spawned by this stream.
  * @param parentDStreams All the parent DStreams of the output DStream.
  */
case class MasterStreamData(
    streamID: Int,
    relatedJobs: Set[Int] = Set[Int](),
    parentDStreams: scala.collection.immutable.Set[Int]
      = scala.collection.immutable.Set[Int]())
{
  /**
    * Deciders, which are StreamingStrategies by default for each
    * inner stage. Stages are identified in a lazy manner when a task finished.
    * A task holds a corresponding DStream ID, which defines a reoccurring
    * stage in a mini-batch.
    */
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

case class MasterJobData(
  jobID: Int,
  stream: Stream)


private[spark] class StreamingRepartitioningTrackerMaster(
  override val rpcEnv: RpcEnv,
  conf: SparkConf)
extends RepartitioningTrackerMaster(rpcEnv, conf) {

  private val _streamData = mutable.HashMap[Int, MasterStreamData]()

  private val _jobData = mutable.HashMap[Int, MasterJobData]()

  private var isInitialized = false

  /**
    * For streaming mini-batches only.
    *
    * We use the DStreamGraph from StreamingContext to look up the DStream
    * and change the partitioner.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logInfo(s"Job end detected with job ID ${jobEnd.jobId}!")
    _jobData.get(jobEnd.jobId).foreach { masterJobData =>
      logInfo("Job finished is a part of a streaming job with stream ID " +
              s"${masterJobData.stream.ID}.")

      val streamData = _streamData.getOrElse(masterJobData.stream.ID,
        throw new SparkException("Streaming data is malformed!"))

      streamData.strategies.foreach {
        _._2.repartition()
      }
    }
  }

  /**
    * Job properties are set to streaming when an output stream
    * generates the job. For example ForeachDStream.
    *
    * If this is the first mini-batch for a stream, the MasterStreamData
    * is created alongside with the job data, do be able to link a job
    * to a specific stream later on, when the mini-batch finishes.
    *
    * Streaming stategies are also sent around to workers.
    */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    /**
      * Finds the dependencies of a set of DStreams recursively.
      * Returns with a set of DStreams found.
      */
    def getDependenciesReqursively(streams: scala.collection.immutable.Set[DStream[_]]):
      scala.collection.immutable.Set[DStream[_]] =
    {
      if (streams.isEmpty) {
        scala.collection.immutable.Set[DStream[_]]()
      } else {
        streams ++ getDependenciesReqursively(streams.flatMap(_.dependencies))
      }
    }

    val outputStreams = StreamingContext.getActive().get.graph.getOutputStreams()

    if(!isInitialized) {
      StreamingUtils.initialize(StreamingContext.getActive().get.graph.getOutputStreams())
      isInitialized = true
    }

    /**
      * Job properties should be set by the RDD's properties on job submission.
      * A property "stream" should be present if it has been submitted by the
      * Spark Streaming module.
      */
    val streamProperties = jobStart.jobProperties.get("stream")
    if (streamProperties.isDefined) {
      val stream = streamProperties.get.asInstanceOf[Stream]
      logInfo(s"Job detected as a mini-batch for DStream with ID ${stream.ID}.")
      val streamID = stream.ID

      /**
        * This is the first time the stream is identified from a job.
        */
      if (!_streamData.contains(streamID)) {
        logInfo(s"Registering stream with DStream ID $streamID.")

        val parentStreams =
          getDependenciesReqursively(
            StreamingContext.getActive().get.graph.getOutputStreams().toSet
          ).map(_.id)

        _streamData.update(streamID,
          MasterStreamData(streamID, Set[Int](jobStart.jobId), parentStreams))
        _jobData.update(jobStart.jobId,
          MasterJobData(jobStart.jobId, stream))

        val scanStrategy = new StreamingStrategy(streamID, stream,
          perBatchSamplingRate = SparkEnv.get.conf.getInt(
            "spark.repartitioning.streaming.per-batch-sampling-rate", 5))
        workers.values.foreach(
          _.reference.send(StreamingScanStrategy(streamID, scanStrategy, parentStreams)))
      } else {
        _streamData.update(streamID, {
          _streamData(streamID).addJob(jobStart.jobId)
        })
        _jobData.update(jobStart.jobId, MasterJobData(jobStart.jobId, stream))
      }
    } else {
      logInfo(s"Regular job start detected, with ID ${jobStart.jobId}. " +
              s"Doing nothing special.")
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    this.synchronized {
      if (!stageSubmitted.stageInfo.rddInfos.head.properties.contains("stream")) {
        super.onStageSubmitted(stageSubmitted)
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.synchronized {
      if (!stageCompleted.stageInfo.rddInfos.head.properties.contains("stream")) {
        super.onStageCompleted(stageCompleted)
      }
    }
  }

  def updateLocalHistogramForStreaming(
    stream: Stream,
    taskInfo: TaskInfo,
    dataCharacteristics: DataCharacteristicsAccumulator
  ): Unit = {
    _streamData.find { _._2.hasParent(stream.ID) } match {
      case Some((sID, streamData)) =>
        logInfo(s"Updating local histogram for task ${taskInfo.taskId} " +
                s"in stream ${stream.ID} with output stream ID $sID.")
        streamData.strategies.getOrElseUpdate(
          stream.ID, new StreamingStrategy(stream.ID, stream, totalSlots.intValue())
        ).onHistogramArrival(taskInfo.index, dataCharacteristics)
      case None => logWarning(
        s"Could not update local histogram for streaming," +
        s" since streaming data does not exist for DStream" +
        s" ID ${stream.ID}!")
    }
  }

  private def updatePartitionMetrics(
    stream: Stream,
    taskInfo: TaskInfo,
    recordsRead: Long
  ): Unit = {
    _streamData.find {
      _._2.hasParent(stream.ID)
    } match {
      case Some((sID, streamData)) =>
        logInfo(s"Updating partition metrics for task ${taskInfo.taskId} " +
          s"in stream ${stream.ID}.")
        val id = stream.ID
        streamData.strategies.getOrElseUpdate(id, new StreamingStrategy(stream.ID, stream, totalSlots.intValue()))
          .asInstanceOf[StreamingStrategy].onPartitionMetricsArrival(taskInfo.index, recordsRead)
      case None => logWarning(
        s"Could not update local histogram for streaming," +
          s" since streaming data does not exist for DStream" +
          s" ID ${stream.ID}!")
    }
  }

  private def getNumberOfPartitions(streamID: Int): Int = {
    // Only one children is assumed
    StreamingUtils.getChildren(streamID) match {
      case head :: tail => head match {
        case sds: ShuffledDStream[_, _, _] => sds.partitioner.numPartitions
        case _ => throw new SparkException("Not a shuffled DStream!")
      }
      case Nil =>
        throw new SparkException("DStream not found in DStreamGraph!")
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
      taskEnd.taskInfo.stageProperties.flatMap(
        _.get("stream").asInstanceOf[Option[Stream]]
      ) match {
        case Some(stream) =>
          StreamingUtils.updateStreamIDToStreamMap(stream.ID, stream)
          if (taskEnd.taskType != "ResultTask") {
            val streamID = stream.ID
            logInfo(s"Task ended with DStream ID of $streamID.")
            val shuffleWriteMetrics = taskEnd.taskMetrics.shuffleWriteMetrics
            val size = shuffleWriteMetrics.dataCharacteristics.value.size
            val recordsPassed = shuffleWriteMetrics.dataCharacteristics.recordsPassed
            logInfo(s"DataCharacteristics size is $size with $recordsPassed records passed.")
            updateLocalHistogramForStreaming(
              stream,
              taskEnd.taskInfo,
              shuffleWriteMetrics.dataCharacteristics)
          }
          val shuffleReadMetrics = taskEnd.taskMetrics.shuffleReadMetrics
          val recordsRead = shuffleReadMetrics.recordsRead
          if (recordsRead > 0) {
            StreamingUtils.getShuffleHead(StreamingUtils.getDStream(stream.ID)) match {
              case Some(shuffleHead) => StreamingUtils.getParents(shuffleHead).foreach(s => updatePartitionMetrics(s,
                taskEnd.taskInfo, recordsRead))
              case None => throw new RuntimeException(s"Cannot find shuffle head for stream $stream")
            }
          }
        case None =>
          super.onTaskEnd(taskEnd)
      }
    }
  }

  /**
    * Initializes a local worker and asks it to register with this
    * repartitioning tracker master.
    */
  override def initializeLocalWorker(): Unit = {
    val worker = new StreamingRepartitioningTrackerWorker(rpcEnv, conf, "driver")
    worker.master = self
    worker.register()
    localWorker = Some(worker)
  }
}


/**
  * A simple strategy to decide when and how to repartition a stage.
  */
class StreamingStrategy(
  streamID: Int,
  stream: Stream,
  var numPartitions: Int = 1,
  val perBatchSamplingRate: Int = 1) extends Decider(streamID) {
  private val partitionerHistory = scala.collection.mutable.Seq[Partitioner]()
  private val histogramComparisionTreshold = 0.01d
  private val partitionHistogram = mutable.HashMap[Int, Long]()
  private var latestPartitioningInfo: Option[PartitioningInfo] = None

  def zeroTime: Time = stream.time

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

  // TODO check distance from uniform, fall back to hashPartitioning if close

  private def isSignificantChange(partitioningInfo: Option[PartitioningInfo], partitionHistogram: Seq[Double],
    treshold: Double): Boolean = {
    val sCut = partitioningInfo.map(_.sCut).getOrElse(0)

    val maxInSCut: Double = if (sCut == 0) {
      1.0d / numPartitions
    } else {
      partitionHistogram.take(sCut).max
    }
    val maxOutsideSCut: Double = partitionHistogram.drop(sCut).max
    maxInSCut + treshold < maxOutsideSCut
  }

  override protected def preDecide(): Boolean = {
    logInfo(s"Deciding if need any repartitioning now for stream " +
      s"with ID $streamID.", "DRRepartitioner")
    logInfo(s"Number of received histograms: ${histograms.size}", "DRHistogram")
    if (histograms.size < SparkEnv.get.conf.getInt("spark.repartitioning.histogram-threshold", 2)) return false

    val orderedPartitionHistogram = partitionHistogram.toSeq.sortBy(_._1).map(_._2).padTo(numPartitions, 0L)
    val sum = orderedPartitionHistogram.sum
    isSignificantChange(latestPartitioningInfo, orderedPartitionHistogram.map(_.toDouble / sum),
      histogramComparisionTreshold)
  }

  override protected def decideAndValidate(globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    isValidHistogram(globalHistogram)
  }

  override protected def getPartitioningInfo(globalHistogram: scala.collection.Seq[(Any, Double)]): PartitioningInfo = {
    // TODO add number of slots to Decider's constructur
    val helperInfo = PartitioningInfo.newInstance(globalHistogram, numPartitions, treeDepthHint)
    val startingLevel = helperInfo.level
    val multiplier = helperInfo.level / helperInfo.sortedValues.head
    numPartitions = numPartitions * multiplier.ceil.toInt
    val partitioningInfo = PartitioningInfo.newInstance(globalHistogram, numPartitions, treeDepthHint)
    logInfo(s"partitioning info: $partitioningInfo", "DRHistogram")
    latestPartitioningInfo = Some(partitioningInfo)
    partitioningInfo
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
    clearHistograms()
  }
}

case class RepartitioningStreamData(
  streamID: Int,
  strategy: StreamingStrategy,
  parentStreams: collection.immutable.Set[Int])

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
          false
      }
    } else {
      super.isDataAware(rdd)
    }
  }
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

object StreamingUtils {
  private val streamIDToDStream = mutable.HashMap[Int, DStream[_]]()
  private val streamIDToStream = mutable.HashMap[Int, Stream]()
  private val streamIDToChildren = mutable.HashMap[Int, Seq[DStream[_]]]()

  def initialize(outputDStreams: Seq[DStream[_]]): Unit = {
    outputDStreams.foreach(ds => {
      streamIDToDStream.update(ds.id, ds)
      ds.dependencies.foreach(dep => {
        val children = streamIDToChildren.getOrElseUpdate(dep.id, Seq.empty[DStream[_]])
        streamIDToChildren.update(dep.id, children :+ ds)
      })
      initialize(ds.dependencies)
    })

  }

  //  def lookupStreamChildrenReqursively(id: Int, streams: Array[DStream[_]]): Option[DStream[_]] = {
  //    if (streams.isEmpty) {
  //      None
  //    } else {
  //      streams.find(stream => stream.dependencies.exists(_.id == id)) match {
  //        case Some(dstream) => Some(dstream)
  //        case None =>
  //          lookupStreamChildrenReqursively(id, streams.flatMap(_.dependencies))
  //      }
  //    }
  //  }

  def updateStreamIDToStreamMap(streamId: Int, stream: Stream) = {
    streamIDToStream.update(streamId, stream)
  }

  def getDStream(streamId: Int): DStream[_] = {
    streamIDToDStream.get(streamId) match {
      case Some(ds) => ds
      case None => throw new RuntimeException(s"Cannot find DStream for id $streamId")
    }
  }

  def getChildren(streamId: Int): Seq[DStream[_]] = {
    streamIDToChildren.get(streamId) match {
      case Some(children) => children
      case None => throw new RuntimeException(s"Cannot find children DStreams for id $streamId")
    }
  }

  def getParents(dStream: ShuffledDStream[_, _, _]): Seq[Stream] = {
    dStream.dependencies.map(dep => streamIDToStream(dep.id))
  }

  // finds only one shuffle head
  def getShuffleHead(dStream: DStream[_]): Option[ShuffledDStream[_, _, _]] = {
    def findShuffleHead(dStreams: Seq[DStream[_]]): Option[ShuffledDStream[_, _, _]] = {
      if (dStreams.nonEmpty) {
        dStreams.find(stream => stream.isInstanceOf[ShuffledDStream[_, _, _]]) match {
          case Some(sds) => Some(sds.asInstanceOf[ShuffledDStream[_, _, _]])
          case None => findShuffleHead(dStreams.flatMap(_.dependencies))
        }
      } else {
        None
      }
    }
    findShuffleHead(Seq(dStream))

    //    dStream match {
    //      case sds: ShuffledDStream[_, _, _] => Some(sds)
    //      case ds => ds.dependencies match {
    //        case head :: tail => getShuffleHead(head)
    //        case Nil => None
    //      }
    //    }
  }
}
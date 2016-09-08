package org.apache.spark.streaming.repartitioning

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.repartitioning.{RepartitioningTrackerFactory, RepartitioningTrackerMaster, ScanStrategies}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ShuffledDStream, Stream}
import org.apache.spark.streaming.repartitioning.decider.{StreamingDeciderFactory, NaivRetentiveStrategy}
import org.apache.spark.util.{DataCharacteristicsAccumulator, Utils}

import scala.collection.mutable

private[spark] class StreamingRepartitioningTrackerMaster(
  override val rpcEnv: RpcEnv,
  conf: SparkConf)
extends RepartitioningTrackerMaster(rpcEnv, conf) {

  private val _streamData = mutable.HashMap[Int, MasterStreamData]()

  private val _jobData = mutable.HashMap[Int, MasterJobData]()

  private var isInitialized = false

  private val deciderFactoryClass =
    Utils.classForName(
      conf.get(
        "spark.repartitioning.streaming.decider.factory",
        "org.apache.spark.repartitioning.streaming.decider.NaivRetentiveStrategy")
    ).asInstanceOf[Class[StreamingDeciderFactory]]

  logInfo(s"The decider factory class is [${deciderFactoryClass.getClass.getName}].")

  private val deciderFactory = deciderFactoryClass.newInstance()

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

        val scanStrategy = deciderFactory(streamID, stream,
          perBatchSamplingRate = SparkEnv.get.conf.getInt(
            "spark.repartitioning.streaming.per-batch-sampling-rate", 5))

        _streamData.update(streamID,
          MasterStreamData(streamID, mutable.Set[Int](jobStart.jobId), parentStreams, scanStrategy))
        _jobData.update(jobStart.jobId,
          MasterJobData(jobStart.jobId, stream))

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

  override protected def replyWithStrategies(workerReference: RpcEndpointRef): Unit = {
    workerReference.send(ScanStrategies(
      _stageData.map(_._2.scanStrategy).toList ++
        _streamData.map {
          streamData => StreamingScanStrategy(
            streamData._2.streamID, streamData._2.scanStrategy, streamData._2.parentDStreams)
        }
    ))
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
          stream.ID,
          deciderFactory(stream.ID, stream, 1, Some(() => getTotalSlots))
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
        streamData.strategies.getOrElseUpdate(
          id,
          deciderFactory(stream.ID, stream, totalSlots.intValue())
        ).asInstanceOf[NaivRetentiveStrategy].onPartitionMetricsArrival(taskInfo.index, recordsRead)
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
            logObject("taskEnd", streamID, taskEnd.taskInfo)
            updateLocalHistogramForStreaming(
              stream,
              taskEnd.taskInfo,
              shuffleWriteMetrics.dataCharacteristics)
          }
          val shuffleReadMetrics = taskEnd.taskMetrics.shuffleReadMetrics
          val recordsRead = shuffleReadMetrics.recordsRead
          if (recordsRead > 0) {
            StreamingUtils.getShuffleHead(StreamingUtils.getDStream(stream.ID)) match {
              case Some(shuffleHead) =>
                StreamingUtils.getParents(shuffleHead).foreach(
                  s => updatePartitionMetrics(s, taskEnd.taskInfo, recordsRead)
                )
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


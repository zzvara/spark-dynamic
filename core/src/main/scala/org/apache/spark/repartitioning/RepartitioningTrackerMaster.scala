package org.apache.spark.repartitioning

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark._
import org.apache.spark.internal.ColorfulLogging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.collection.mutable

/**
  * Tracks and aggregates histograms of certain tasks of jobs, where dynamic repartitioning
  * is enabled.
  *
  * @todo Currently repartitioning mode is compatible only for one job!
  */
private[spark] class RepartitioningTrackerMaster(
                                                  override val rpcEnv: RpcEnv,
                                                  conf: SparkConf)
extends RepartitioningTracker(conf) with ColorfulLogging {
  /**
    * Collection of repartitioning workers. We expect them to register.
    */
  protected val workers = mutable.HashMap[String, Worker]()
  /**
    * Local worker in case when running in local mode.
    */
  protected var localWorker: Option[RepartitioningTrackerWorker] = None

  /**
    * Final histograms recorded by repartitioning workers.
    * This can be switched with configuration
    * `spark.repartitioning.final-histgorams`. Default value is false.
    */
  protected val finalHistograms =
    mutable.HashMap[Int, mutable.HashMap[Long, DataCharacteristicsAccumulator]]()

  var doneRepartitioning = false

  /**
    * Pending stages to dynamically repartition. These stages are currently
    * running and we're waiting their tasks' histograms to arrive.
    * It also contains repartitioning strategies for stages.
    */
  protected val _stageData = mutable.HashMap[Int, MasterStageData]()

  // TODO make this mode stagewise configurable
  private val configuredRPMode =
    if (conf.getBoolean("spark.repartitioning", true)) {
      if (conf.getBoolean("spark.repartitioning.only.once", true)) {
        RepartitioningModes.ONLY_ONCE
      } else {
        RepartitioningModes.ON
      }
    } else {
      RepartitioningModes.OFF
    }

  protected val totalSlots: AtomicInteger = new AtomicInteger(0)

  def getTotalSlots: Int = totalSlots.get()

  /**
    * Initializes a local worker and asks it to register with this
    * repartitioning tracker master.
    */
  def initializeLocalWorker(): Unit = {
    val worker = new RepartitioningTrackerWorker(rpcEnv, conf, "driver")
    worker.master = self
    worker.register()
    localWorker = Some(worker)
  }

  /**
    * Gets the local worker.
    */
  def getLocalWorker: Option[RepartitioningTrackerWorker] = localWorker

  protected def replyWithStrategies(workerReference: RpcEndpointRef): Unit = {
    workerReference.send(ScanStrategies(
      _stageData.map(_._2.scanStrategy).toList
    ))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    this.synchronized {
      case Register(executorID, workerReference) =>
        logInfo(s"Received register message for worker $executorID", "DRCommunication")
        if (workers.contains(executorID)) {
          logWarning(s"Attempt to register worker {$executorID} twice!", "DRCommunication")
          context.reply(false)
        } else {
          logInfo(s"Registering worker from executor {$executorID}.", "DRCommunication")
          workers.put(executorID, new Worker(executorID, workerReference))
          context.reply(true)
          replyWithStrategies(workerReference)
        }

      /**
        * The case when a worker sends histogram snapshot of a task.
        *
        * We need to identify the stage that this particular task
        * belongs to.
        */
      case ShuffleWriteStatus(stageID, taskID, partitionID,
                              keyHistogram: DataCharacteristicsAccumulator) =>
        logInfo(s"Received ShuffleWriteStatus message for " +
          s"stage $stageID and task $taskID", "DRCommunication")
        _stageData.get(stageID) match {
          case Some(stageData) =>
            logInfo(s"Received key histogram for stage $stageID" +
              s" task $taskID (with size ${keyHistogram.value.size}).",
              "DRCommunication", "DRHistogram")
            logDebug(s"Histogram content is:", "DRHistogram")
            logDebug(keyHistogram.value.map(_.toString).mkString("\n"), "DRHistogram")
            stageData.strategy.onHistogramArrival(partitionID, keyHistogram)
            context.reply(true)
          case None =>
            logWarning(s"Histograms arrived for invalid stage $stageID.",
              "DRCommunication", "DRHistogram")
            context.reply(false)
        }
    }
  }

  /**
    * When we see that a new stage is starting, we should get the configuration for the given job,
    * to check whether dynamic repartitioning is enabled. If so, then we should track submitted
    * tasks to that stage and announce a global watch for its tasks throughout the cluster.
    *
    * We need to send a notice to each worker to track all tasks for a specific stage.
    *
    * @todo Load job strategies.
    */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    this.synchronized {
      val stageInfo = stageSubmitted.stageInfo
      val jobID = stageInfo.jobId
      val partitioner = stageInfo.partitioner
      val repartitioningMode = if (stageInfo.isInstanceOf[ResultStageInfo] ||
        partitioner.isEmpty)
      {
        RepartitioningModes.OFF
      } else {
        configuredRPMode
      }
      if (repartitioningMode == RepartitioningModes.OFF) {
        logInfo(s"A stage submitted, but dynamic repartitioning is switched off.",
                "DRCommunication")
      } else {
        val stageID = stageInfo.stageId
        logInfo(s"A stage with id $stageID (job ID is $jobID)" +
                s"submitted with dynamic repartitioning " +
                s"mode $repartitioningMode.", "DRCommunication")
        val scanStrategy = StandaloneStrategy(stageID,
          new ThroughputPrototype(totalSlots.intValue()))
        _stageData.update(stageID,
          MasterStageData(stageInfo,
            new Strategy(stageID, stageInfo.attemptId, totalSlots.intValue()),
            repartitioningMode,
            scanStrategy))
        logInfo(s"Sending repartitioning scan-strategy to each worker for " +
                s"job $stageID", "DRCommunication")
        workers.values.foreach(_.reference.send(scanStrategy))
      }
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    totalSlots.addAndGet(executorAdded.executorInfo.totalCores)
    logInfo(s"Executor added. Total cores is ${totalSlots.intValue()}.")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    totalSlots.addAndGet(-executorRemoved.executorInfo.totalCores)
    logInfo(s"Executor removed. Total cores is ${totalSlots.intValue()}.")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.synchronized {
      val stageID = taskEnd.stageId
      if (_stageData.contains(stageID)) {
        if (taskEnd.reason == Success) {
          // Currently we disable repartitioning for a stage,
          // if any of its tasks finish.
          logInfo(s"A task completion detected for stage $stageID. " +
            s"Clearing tracking.", "DRCommunication")
          if(!doneRepartitioning) {
            shutDownScanners(stageID)
            if (_stageData(stageID).mode == RepartitioningModes.ONLY_ONCE) {
              doneRepartitioning = true
            }
          }
        } else {
          logWarning(s"Detected completion of a failed task for " +
                     s"stage $stageID!", "DRCommunication")
        }
      } else {
        logWarning(s"Invalid stage of id $stageID detected on task completion! " +
          s"Maybe not tracked intentionally?", "DRCommunication")
      }
    }
  }

  def shutDownScanners(stageId: Int): Unit = {
    logInfo(s"Shutting down scanners for stage $stageId.", "DRCommunication")
    workers.values.foreach(_.reference.send(ShutDownScanners(stageId)))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.synchronized {
      val stageID = stageCompleted.stageInfo.stageId
      workers.values.foreach(_.reference.send(ClearStageData(stageID)))
      _stageData.remove(stageID) match {
        case Some(_) =>
          if (stageCompleted.stageInfo.getStatusString == "succeeded") {
            // Currently we disable repartitioning for a stage, if any of its tasks finish.
            logInfo(s"A stage completion detected for stage $stageID." +
                    s"Clearing tracking.", "DRMaster")
            // TODO Remove stage data from workers.
          } else {
            logWarning(s"Detected completion of a failed stage with id $stageID", "DRCommunication")
          }
        case None => logWarning(s"Invalid stage of id $stageID detected on stage completion!",
                                "DRCommunication")
      }
    }
  }

  /**
    * Broadcasts a repartitioning strategy to each worker for a given stage.
    */
  def broadcastRepartitioningStrategy(stageID: Int,
                                      repartitioner: Partitioner, version: Int): Unit = {
    logInfo(s"Sending repartitioning strategy back to each worker for stage $stageID",
            "DRCommunication", "DRRepartitioner")
    workers.values.foreach(
      _.reference.send(RepartitioningStrategy(stageID, repartitioner, version)))
  }
}

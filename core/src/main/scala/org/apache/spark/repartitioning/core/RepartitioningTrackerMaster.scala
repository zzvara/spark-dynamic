package org.apache.spark.repartitioning.core

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Partitioner
import org.apache.spark.repartitioning.MasterStageData
import org.apache.spark.repartitioning.core.messaging._
import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.collection.mutable

import scala.language.reflectiveCalls

abstract class RepartitioningTrackerMaster[
  ComponentReference <: Messageable,
  CallContext <: { def reply(response: Any): Unit },
  TaskContext <: TaskContextInterface[TaskMetrics],
  TaskMetrics <: TaskMetricsInterface[TaskMetrics],
  Operator]()(
  implicit ev1: ScannerFactory[Scanner[TaskContext, TaskMetrics]])
extends RepartitioningTracker[ComponentReference] {
  type RTW = RepartitioningTrackerWorker[
    ComponentReference,
    ComponentReference,
    TaskContext,
    TaskMetrics,
    Operator]

  /**
    * Collection of repartitioning workers. We expect them to register.
    */
  protected val workers = mutable.HashMap[String, WorkerReference[ComponentReference]]()

  /**
    * Local worker in case when running in local mode.
    */
  protected var localWorker: Option[RTW] = None
  /**
    * Final histograms recorded by repartitioning workers.
    * This can be switched with configuration
    * `spark.repartitioning.final-histograms`. Default value is false.
    * @todo Not used currently.
    */
  private val finalHistograms =
    mutable.HashMap[Int, mutable.HashMap[Long, DataCharacteristicsAccumulator]]()

  private var doneRepartitioning = false

  /**
    * Pending stages to dynamically repartition. These stages are currently
    * running and we're waiting their tasks' histograms to arrive.
    * It also contains repartitioning strategies for stages.
    */
  protected val _stageData = mutable.HashMap[Int, MasterStageData[TaskContext, TaskMetrics]]()

  /**
    * @todo Make this stage-wise configurable.
    */
  protected val configuredRPMode: RepartitioningModes.Value =
    if (Configuration.internal().getBoolean("repartitioning.enabled")) {
      if (Configuration.internal().getBoolean("repartitioning.batch.only-once")) {
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
  def initializeLocalWorker(): Unit

  /**
    * Gets the local worker.
    */
  def getLocalWorker: Option[RTW] = localWorker

  protected def replyWithStrategies(workerReference: ComponentReference): Unit = {
    workerReference.send(
      ScanStrategies(_stageData.map(_._2.scanStrategy).toList)
    )
  }

  protected def componentReceiveAndReply(context: CallContext): PartialFunction[Any, Unit] = {
    this.synchronized {
      case Register(executorID, workerReference) =>
        logInfo(s"Received register message for worker $executorID", "DRCommunication")
        if (workers.contains(executorID)) {
          logWarning(s"Attempt to register worker {$executorID} twice!", "DRCommunication")
          context.reply(false)
        } else {
          logInfo(s"Registering worker from executor {$executorID}.", "DRCommunication")
          workers.put(executorID,
                      new WorkerReference[ComponentReference](executorID,
                        workerReference.asInstanceOf[ComponentReference]))
          context.reply(true)
          replyWithStrategies(workerReference.asInstanceOf[ComponentReference])
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

  protected def whenStageSubmitted(jobID: Int,
                                   stageID: Int,
                                   attemptID: Int,
                                   repartitioningMode: RepartitioningModes.Value): Unit = {
    this.synchronized {
      if (repartitioningMode == RepartitioningModes.OFF) {
        logInfo(s"A stage submitted, but dynamic repartitioning is switched off.",
                "DRCommunication")
      } else {
        logInfo(s"A stage with id $stageID (job ID is $jobID)" +
                s"submitted with dynamic repartitioning " +
                s"mode $repartitioningMode.", "DRCommunication")
        val scanStrategy = StandaloneStrategy[TaskContext, TaskMetrics](
          stageID,
          implicitly[ScannerFactory[Scanner[TaskContext, TaskMetrics]]].apply(totalSlots.intValue())
        )
        _stageData.update(stageID,
          MasterStageData(stageID,
            new Strategy(stageID, attemptID, totalSlots.intValue()),
            repartitioningMode,
            scanStrategy))
        logInfo(s"Sending repartitioning scan-strategy to each worker for " +
                s"job $stageID", "DRCommunication")
        workers.values.foreach(_.reference.send(scanStrategy))
      }
    }
  }

  protected def whenTaskEnd(stageID: Int, reason: TaskEndReason.Value): Unit = this.synchronized {
    if (_stageData.contains(stageID)) {
      if (reason == TaskEndReason.Success) {
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

  protected def whenStageCompleted(stageID: Int, reason: StageEndReason.Value): Unit =
    this.synchronized {
      workers.values.foreach(_.reference.send(ClearStageData(stageID)))
      _stageData.remove(stageID) match {
        case Some(_) =>
          if (reason == StageEndReason.Success) {
            // Currently we disable repartitioning for a stage, if any of its tasks finish.
            logInfo(s"A stage completion detected for stage $stageID." +
                    s"Clearing tracking.", "DRMaster")
            /**
              * @todo Remove stage data from workers.
              */
          } else {
            logWarning(s"Detected completion of a failed stage with id $stageID", "DRCommunication")
          }
        case None => logWarning(s"Invalid stage of id $stageID detected on stage completion!",
                                "DRCommunication")
      }
    }

  private def shutDownScanners(stageID: Int): Unit = {
    logInfo(s"Shutting down scanners for stage $stageID.", "DRCommunication")
    workers.values.foreach(_.reference.send(ShutDownScanners(stageID)))
  }

  /**
    * Broadcasts a repartitioning strategy to each worker for a given stage.
    * @todo Not used?
    */
  def broadcastRepartitioningStrategy(stageID: Int,
                                      repartitioner: Partitioner,
                                      version: Int): Unit = {
    logInfo(s"Sending repartitioning strategy back to each worker for stage $stageID",
            "DRCommunication", "DRRepartitioner")
    workers.values.foreach(
      _.reference.send(RepartitioningStrategy(stageID, repartitioner, version)))
  }
}

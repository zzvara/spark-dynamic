package org.apache.spark.repartitioning.core

import org.apache.spark.executor.RepartitioningInfo
import org.apache.spark.repartitioning.core.messaging._
import org.apache.spark.repartitioning.{RepartitioningStageData, core}
import org.apache.spark.util.{DataCharacteristicsAccumulator, ThreadUtils}
import org.apache.spark.{Partitioner, SparkEnv, SparkException}

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class RepartitioningTrackerWorker[
  MasterReference <: Messageable,
  SelfReference,
  TaskContext <: TaskContextInterface[TaskMetrics],
  TaskMetrics <: TaskMetricsInterface[TaskMetrics],
  Operator](val executorID: String)
extends RepartitioningTracker[MasterReference] {
  protected val stageData =
    mutable.HashMap[Int, RepartitioningStageData[TaskContext, TaskMetrics]]()

  def selfReference: SelfReference

  /**
    * Registers with the Master tracker. Uses `selfReference`.
    */
  def register(): Unit = {
    logInfo("Registering with Master tracker.", "DRCommunication")
    sendTracker(Register(executorID, selfReference))
  }

  /**
    * @todo Use this thread pool to instantiate scanners.
    */
  protected val threadPool =
    ThreadUtils.newDaemonCachedThreadPool("Executor repartitioning scanner worker")

  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (!response) {
      throw new SparkException(
        "Error reply received from RepartitioningTracker. Expecting true, got "
          + response.toString)
    }
  }

  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      master.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with RepartitioningTracker", e, "DRCommunication")
        throw new SparkException("Error communicating with RepartitioningTracker", e)
    }
  }

  /**
    * Called by Executors when on task arrival.
    */
  def taskArrival(taskID: Long, stageID: Int, taskContext: TaskContext): Unit = {
    this.synchronized {
      logInfo(s"Task arrived with ID $taskID for stage $stageID.", "DRCommunication")
      stageData.get(stageID) match {
        case Some(sd) =>
          val repartitioningInfo =
          // We can only give the TaskMetrics at this point.
          // ShuffleWriteMetrics has not been initialized.
            new RepartitioningInfo[TaskMetrics](stageID, taskID, executorID,
              taskContext.taskMetrics(),
              sd.partitioner, sd.version)
          taskContext.taskMetrics().repartitioningInfo = Some(repartitioningInfo)

          if (!sd.isRepartitioningFinished) {
            logInfo(s"Found strategy for stage $stageID, task $taskID. " +
                    s"Instantiating scanner for its context.", "DRRepartitioner")
            /**
              * @todo Warning! Scanner prototype initialized with 0 total slots.
              */
            val scanner = sd.scanner.apply(0, sendHistogram)
            scanner.taskContext = taskContext
            val thread = new Thread(scanner)
            thread.start()
            logInfo(s"Scanner started for task $taskID.")
            sd.scannedTasks = Some(
              sd.scannedTasks.get + (
                taskID -> WorkerTaskData[TaskContext, TaskMetrics](repartitioningInfo, scanner)))
          }

          logInfo(s"Added TaskContext $taskContext for stage $stageID task $taskID to" +
                  s"scanned tasks on worker $executorID.", "scannedTasks")
          logInfo(s"Scanned tasks after update on worker $executorID, ${sd.scannedTasks}",
                  "scannedTasks")
        case None =>
          logWarning(s"Task with id $taskID arrived for non-registered stage of id $stageID. " +
                     s"Doing nothing.", "DRCommunication")
      }
    }
  }

  /**
    * Called by scanners to send histograms through the workers to the master.
    */
  def sendHistogram(stageID: Int,
                    taskID: Long,
                    partitionID: Int,
                    keyHistogram: DataCharacteristicsAccumulator): Unit = {
    logInfo(s"Sending histogram (with size ${keyHistogram.value.size})" +
            s" (records passed is ${
              keyHistogram.recordsPassed
            }) " +
            s"to driver for stage $stageID task $taskID",
            "DRCommunication", "DRHistogram")
    sendTracker(ShuffleWriteStatus(stageID, taskID, partitionID, keyHistogram))
  }

  protected def componentReceive: PartialFunction[Any, Unit] = this.synchronized {
    case StandaloneStrategy(stageID, scanner) =>
      val castedScanner = scanner.asInstanceOf[
        core.ScannerFactory[core.Scanner[TaskContext, TaskMetrics]]
      ]
      logInfo(s"Received scan strategy for stage $stageID.", "DRCommunication")
      stageData.put(stageID, RepartitioningStageData[TaskContext, TaskMetrics](castedScanner))
    case RepartitioningStrategy(stageID, repartitioner, version) =>
      logInfo(s"Received repartitioning strategy for" +
              s"stage $stageID with repartitioner $repartitioner.",
              "DRCommunication", "DRRepartitioner", "cyan")
      updateRepartitioners(stageID, repartitioner, version)
      logInfo(s"Finished processing repartitioning strategy for stage $stageID.",
        "cyan")
      if (SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", defaultValue = true)) {
        logInfo("Shutting down scanners because repartitioning mode is set to only-once")
        logInfo(s"Stopping scanners for stage $stageID on executor $executorID.",
          "DRCommunication", "cyan")
        stopScanners(stageID)
      }
    case ScanStrategies(scanStrategies) =>
      logInfo(s"Received a list of scan strategies, with size of ${scanStrategies.length}.")
      scanStrategies.foreach {
        case StandaloneStrategy(stageID,
                                scanner) =>
          stageData.put(stageID, RepartitioningStageData[TaskContext, TaskMetrics](
            scanner.asInstanceOf[core.ScannerFactory[core.Scanner[TaskContext, TaskMetrics]]]))
      }
    case ShutDownScanners(stageID) =>
      logInfo(s"Stopping scanners for stage $stageID on executor $executorID.",
        "DRCommunication", "cyan")
      stopScanners(stageID)
    case ClearStageData(stageID) =>
      logInfo(s"Clearing stage data for stage $stageID on " +
        s"executor $executorID.", "DRCommunication", "cyan")
      clearStageData(stageID)
  }

  private def updateRepartitioners(stageID: Int, repartitioner: Partitioner, version: Int): Unit = {
    stageData.get(stageID) match {
      case Some(sd) =>
        val scannedTasks = sd.scannedTasks.get
        sd.partitioner = Some(repartitioner)
        sd.version = Some(version)
        logInfo(s"Scanned tasks before repartitioning on worker $executorID, ${sd.scannedTasks}",
          "DRRepartitioner")
        logInfo(s"Scanned partitions are" +
          s" ${scannedTasks.values.map(_.scanner.taskContext.partitionID())}",
          "DRRepartitioner")
        scannedTasks.values.foreach(wtd => {
          wtd.info.updateRepartitioner(repartitioner, version)
          logInfo(s"Repartitioner set for stage $stageID task ${wtd.info.taskID} on" +
            s"worker $executorID", "DRRepartitioner")
        })
      case None =>
        logWarning(s"Repartitioner arrived for non-registered stage of id $stageID." +
          s"Doing nothing.", "DRRepartitioner")
    }
  }

  private def clearStageData(stageID: Int): Unit = {
    stageData.get(stageID) match {
      case Some(sd) =>
        if (sd.scannedTasks.nonEmpty) stopScanners(stageID)
        stageData.remove(stageID)
      case None =>
    }
  }

  private def stopScanners(stageID: Int): Unit = {
    stageData.get(stageID) match {
      case Some(sd) =>
        val scannedTasks = sd.scannedTasks
        scannedTasks.foreach(_.foreach({ st =>
          st._2.scanner.stop()
          st._2.info.finishTracking()
        }))
        sd.finishRepartitioning()
      case None =>
        logWarning(s"Attempt to stop scanners for non-registered stage of id $stageID." +
          s"Doing nothing.", "DRScanner")
    }
  }

  def isDataAware(operator: Operator): Boolean = {
    true
  }
}
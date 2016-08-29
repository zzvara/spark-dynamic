/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.executor.{RepartitioningInfo, ShuffleWriteMetrics}
import org.apache.spark.internal.{ColorfulLogging, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.repartitioning._
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.util.{DataCharacteristicsAccumulator, TaskCompletionListener, ThreadUtils}

import scala.collection.mutable
import scala.collection.mutable.{Map, _}
import scala.reflect.ClassTag

/**
  * Common interface for each repartitioning tracker.
  */
private[spark] abstract class RepartitioningTracker(conf: SparkConf)
  extends SparkListener with ColorfulLogging with RpcEndpoint {
  var master: RpcEndpointRef = _
}

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
  protected val _stageData = HashMap[Int, MasterStageData]()

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

private[spark] class RepartitioningTrackerWorker(override val rpcEnv: RpcEnv,
                                                 conf: SparkConf,
                                                 executorId: String)
  extends RepartitioningTracker(conf) with RpcEndpoint with ColorfulLogging {

  protected val stageData = mutable.HashMap[Int, RepartitioningStageData]()
  /**
    * @todo Use this thread pool to instantiate scanners.
    */
  private val threadPool =
    ThreadUtils.newDaemonCachedThreadPool("Executor repartitioning scanner worker")

  rpcEnv.setupEndpoint(RepartitioningTracker.WORKER_ENDPOINT_NAME, this)

  def register(): Unit = {
    logInfo("Registering with Master tracker.", "DRCommunication")
    sendTracker(Register(executorId, self))
  }

  /**
    * Called by Executors when on task arrival.
    */
  def taskArrival(taskID: Long, stageID: Int, taskContext: TaskContext): Unit = {
    this.synchronized {
      logInfo(s"Task arrived with ID $taskID for stage $stageID", "DRCommunication")
      stageData.get(stageID) match {
        case Some(sd) =>
          val repartitioningInfo =
            // We can only give the TaskMetrics at this point.
            // ShuffleWriteMetrics has not been initialized.
            new RepartitioningInfo(stageID, taskID, executorId,
              taskContext.taskMetrics(),
              sd.partitioner, sd.version)
          taskContext.taskMetrics().repartitioningInfo = Some(repartitioningInfo)

          if (!sd.isRepartitioningFinished) {
            logInfo(s"Found strategy for stage $stageID, task $taskID. " +
              s"Instantiating scanner for its context.", "DRRepartitioner")
            val scanner = sd.scannerPrototype.newInstance()
            scanner.taskContext = taskContext
            val thread = new Thread(scanner)
            thread.start()
            logInfo(s"Scanner started for task $taskID.")
            sd.scannedTasks = Some(
              sd.scannedTasks.get + (taskID -> WorkerTaskData(repartitioningInfo, scanner)))
          }

          logInfo(s"Added TaskContext $taskContext for stage $stageID task $taskID to" +
                  s"scanned tasks on worker $executorId.", "scannedTasks")
          logInfo(s"Scanned tasks after update on worker $executorId, ${sd.scannedTasks}",
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
  def sendHistogram(stageID: Int, taskID: Long,
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

  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      master.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with RepartitioningTracker", e, "DRCommunication")
        throw new SparkException("Error communicating with RepartitioningTracker", e)
    }
  }

  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (!response) {
      throw new SparkException(
        "Error reply received from RepartitioningTracker. Expecting true, got "
          + response.toString)
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case StandaloneStrategy(stageID, scanner) =>
      logInfo(s"Received scan strategy for stage $stageID.", "DRCommunication")
      stageData.put(stageID, RepartitioningStageData(scanner))
    case RepartitioningStrategy(stageID, repartitioner, version) =>
      logInfo(s"Received repartitioning strategy for" +
              s"stage $stageID with repartitioner $repartitioner.",
              "DRCommunication", "DRRepartitioner", "cyan")
      updateRepartitioners(stageID, repartitioner, version)
      logInfo(s"Finished processing repartitioning strategy for stage $stageID.",
        "cyan")
      if (SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", defaultValue = true)) {
        logInfo("Shutting down scanners because repartitioning mode is set to only-once")
        logInfo(s"Stopping scanners for stage $stageID on executor $executorId.",
          "DRCommunication", "cyan")
        stopScanners(stageID)
      }
    case ScanStrategies(scanStrategies) =>
      logInfo(s"Received a list of scan strategies, with size of ${scanStrategies.length}.")
      scanStrategies.foreach {
        case StandaloneStrategy(stageID, scanner) =>
          stageData.put(stageID, RepartitioningStageData(scanner))
      }
    case ShutDownScanners(stageID) =>
      logInfo(s"Stopping scanners for stage $stageID on executor $executorId.",
        "DRCommunication", "cyan")
      stopScanners(stageID)
    case ClearStageData(stageID) =>
      logInfo(s"Clearing stage data for stage $stageID on " +
              s"executor $executorId.", "DRCommunication", "cyan")
      clearStageData(stageID)
  }

  private def updateRepartitioners(stageID: Int, repartitioner: Partitioner, version: Int): Unit = {
    stageData.get(stageID) match {
      case Some(sd) =>
        val scannedTasks = sd.scannedTasks.get
        sd.partitioner = Some(repartitioner)
        sd.version = Some(version)
        logInfo(s"Scanned tasks before repartitioning on worker $executorId, ${sd.scannedTasks}",
          "DRRepartitioner")
        logInfo(s"Scanned partitions are" +
          s" ${scannedTasks.values.map(_.scanner.taskContext.partitionId())}",
          "DRRepartitioner")
        scannedTasks.values.foreach(wtd => {
          wtd.info.updateRepartitioner(repartitioner, version)
          logInfo(s"Repartitioner set for stage $stageID task ${wtd.info.taskID} on" +
            s"worker $executorId", "DRRepartitioner")
        })
      case None =>
        logWarning(s"Repartitioner arrived for non-registered stage of id $stageID." +
          s"Doing nothing.", "DRRepartitioner")
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

  private def clearStageData(stageID: Int): Unit = {
    stageData.get(stageID) match {
      case Some(sd) =>
        if (sd.scannedTasks.nonEmpty) stopScanners(stageID)
        stageData.remove(stageID)
      case None =>
    }
  }

  def isDataAware(rdd: RDD[_]): Boolean = {
    true
  }
}

private[spark] object RepartitioningTracker extends Logging {
  val MASTER_ENDPOINT_NAME = "RepartitioningTrackerMaster"
  val WORKER_ENDPOINT_NAME = "RepartitioningTrackerWorker"

  type Histogram[T] = mutable.Map[T, Double]
}

trait ScannerPrototype extends Serializable {
  def newInstance(): Scanner
}

/**
  * Decides when to send the histogram to the master from the workers.
  *
  * This strategy should run somewhere near the TaskMetrics and should decide
  * based on many factors when to send the histogram to the master.
  * Also, it should declare the sampling method.
  */
abstract class Scanner(val totalSlots: Int) extends Serializable
with Runnable with ColorfulLogging {
  var taskContext: TaskContext = _
  var isRunning: Boolean = false

  def stop(): Unit

  def setContext(context: TaskContext): Unit = {
    taskContext = context
  }
}

class ThroughputPrototype(val totalSlots: Int) extends ScannerPrototype {
  def newInstance(): Scanner = new Throughput(totalSlots)
}

class Throughput(override val totalSlots: Int) extends Scanner(totalSlots) {
  private var lastHistogramHeight: Long = 0
  private val keyHistogramWidth: Int =
    SparkEnv.get.conf.getInt("spark.repartitioning.key-histogram.truncate", 50)

  override def stop(): Unit = {
    isRunning = false
  }

  def updateTotalSlots(shuffleWriteMetrics: ShuffleWriteMetrics): Unit = {
    logInfo("Updating number of total slots.")
    shuffleWriteMetrics.dataCharacteristics.updateTotalSlots(totalSlots)
  }

  override def run(): Unit = {
    logInfo(s"Running scanner for stage ${taskContext.stageId()} task" +
            s" ${taskContext.taskAttemptId()}.", "default", "strongBlue")
    require(taskContext != null, "Scanner needs to have a valid task context!")
    isRunning = true
    taskContext.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        logInfo(s"Detected completion for stage ${taskContext.stageId()} task" +
                s" ${taskContext.taskAttemptId()}.", "DRCommunication")
        isRunning = false
      }
    })

    updateTotalSlots(taskContext.taskMetrics().shuffleWriteMetrics)

    Thread.sleep(SparkEnv.get.conf.getInt("spark.repartitioning.throughput.interval", 1000))
    while (isRunning) {
      val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
      val dataCharacteristics = shuffleWriteMetrics.dataCharacteristics
      val recordBound =
        SparkEnv.get.conf.getInt("spark.repartitioning.throughput.record-bound", 100)
      val histogramHeightDelta = dataCharacteristics.recordsPassed - lastHistogramHeight
      if (dataCharacteristics.width == 0) {
        logInfo(s"Histogram is empty for task ${taskContext.taskAttemptId()}. " +
                s"Doing Nothing.", "DRHistogram")
      } else if (recordBound > histogramHeightDelta) {
        logInfo(s"Not enough records ($histogramHeightDelta) " +
                s"processed to send the histogram to the driver.", "DRHistogram")
      } else {
        lastHistogramHeight = dataCharacteristics.recordsPassed
        SparkEnv.get.repartitioningWorker().get
          .sendHistogram(
            taskContext.stageId(),
            taskContext.taskAttemptId(),
            taskContext.partitionId(),
            dataCharacteristics)
      }
      Thread.sleep(SparkEnv.get.conf.getInt("spark.repartitioning.throughput.interval", 1000))
      updateTotalSlots(taskContext.taskMetrics().shuffleWriteMetrics)
    }
    logInfo(s"Scanner is finishing for stage ${taskContext.stageId()} task" +
            s" ${taskContext.taskAttemptId()}.", "default", "strongBlue")
  }
}

/**
  * A simple strategy to decide when and how to repartition a stage.
  */
class Strategy(stageID: Int,
  attemptID: Int,
  var numPartitions: Int)
  extends Decider(stageID) {

  /**
    * Called by the RepartitioningTrackerMaster if new histogram arrives
    * for this particular job's strategy.
    */
  override def onHistogramArrival(partitionID: Int,
    keyHistogram: DataCharacteristicsAccumulator): Unit = {
    this.synchronized {
      if (keyHistogram.version == currentVersion) {
        logInfo(s"Recording histogram arrival for partition $partitionID.",
          "DRCommunication", "DRHistogram")
        if (!SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", true) ||
          repartitionCount == 0) {
          logInfo(s"Updating histogram for partition $partitionID.", "DRHistogram")
          histograms.update(partitionID, keyHistogram)
          if (repartition()) {
            repartitionCount += 1
            if (SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", true)) {
              /*
              SparkEnv.get.repartitioningTracker
                .asInstanceOf[RepartitioningTrackerMaster]
                .shutDownScanners(stageID)
                */
            }
          }
        }
      } else if (keyHistogram.version < currentVersion) {
        logInfo(s"Recording outdated histogram arrival for partition $partitionID. " +
          s"Doing nothing.", "DRCommunication", "DRHistogram")
      } else {
        logInfo(s"Recording histogram arrival from a future step for " +
          s"partition $partitionID. Doing nothing.", "DRCommunication", "DRHistogram")
      }
    }
  }

  override protected def clearHistograms(): Unit = {
    // Histogram is not going to be valid while using another Partitioner.
    histograms.clear()
  }

  override protected def preDecide(): Boolean = {
    histograms.size >=
      SparkEnv.get.conf.getInt("spark.repartitioning.histogram-threshold", 2)
  }

  override protected def decideAndValidate(globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    isValidHistogram(globalHistogram)
  }

  override protected def resetPartitioners(newPartitioner: Partitioner): Unit = {
    SparkContext.getOrCreate().dagScheduler.refineChildrenStages(stageID, newPartitioner.numPartitions)
    SparkEnv.get.repartitioningTracker.get
      .asInstanceOf[RepartitioningTrackerMaster]
      .broadcastRepartitioningStrategy(stageID, newPartitioner, currentVersion)
    broadcastHistory += newPartitioner
    logInfo(s"Version of histograms pushed up for stage $stageID", "DRHistogram")
    clearHistograms()
  }
}

class Worker(val executorID: String,
             val reference: RpcEndpointRef)

abstract class RepartitioningTrackerFactory {
  def createMaster(rpcEnv: RpcEnv, conf: SparkConf): RepartitioningTrackerMaster
  def createWorker(rpcEnv: RpcEnv, conf: SparkConf,
                   executorId: String): RepartitioningTrackerWorker
}

class CoreRepartitioningTrackerFactory extends RepartitioningTrackerFactory {
  def createMaster(rpcEnv: RpcEnv, conf: SparkConf): RepartitioningTrackerMaster = {
    new RepartitioningTrackerMaster(rpcEnv, conf)
  }
  def createWorker(rpcEnv: RpcEnv, conf: SparkConf,
                   executorId: String): RepartitioningTrackerWorker = {
    new RepartitioningTrackerWorker(rpcEnv, conf, executorId)
  }
}
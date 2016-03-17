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

import org.apache.spark.AccumulatorParam.DataCharacteristicsAccumulatorParam
import org.apache.spark.RepartitioningTracker.Histogram
import org.apache.spark.executor.RepartitioningInfo
import org.apache.spark.executor.ShuffleWriteMetrics.DataCharacteristics
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.util.{TaskCompletionListener, ThreadUtils}
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

/**
  * Represents a message between repartitioning trackers.
  */
private[spark] sealed trait RepartitioningTrackerMessage

/**
  * Shuffle write status message.
  */
private[spark] case class ShuffleWriteStatus[T](
  stageID: Int,
  taskID: Long,
  partitionID: Int,
  keyHistogram: DataCharacteristics[T]) extends RepartitioningTrackerMessage

private[spark] case class FinalHistogram[T](
  stageID: Int,
  taskID: Long,
  finalHistogram: DataCharacteristics[T]) extends RepartitioningTrackerMessage

/**
  * Registering message sent from workers.
  */
private[spark] case class Register(executorID: String, workerReferece: RpcEndpointRef)
  extends RepartitioningTrackerMessage

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class ScanStrategy(stageID: Int, scanner: ScannerPrototype)
  extends RepartitioningTrackerMessage

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class ShutDownScanners(stageID: Int)
  extends RepartitioningTrackerMessage

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class ClearStageData(stageID: Int)
  extends RepartitioningTrackerMessage

/**
  * Repartitioning strategy message sent to workers.
  */
private[spark] case class RepartitioningStrategy(stageID: Int,
                                                 repartitioner: Partitioner,
                                                 version: Int)
  extends RepartitioningTrackerMessage

/**
  * Enumeration for repartitioning modes. These settings are global right now,
  * not stage based.
  */
object RepartitioningModes extends Enumeration {
  val ON, ONLY_ONCE, OFF = Value
}

case class MasterStageData(info: StageInfo, strategy: Strategy, mode: RepartitioningModes.Value)

case class RepartitioningStageData(
  var scannerPrototype: ScannerPrototype,
  scannedTasks: mutable.HashMap[Long, WorkerTaskData] = mutable.HashMap[Long, WorkerTaskData](),
  var partitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0)){

  var _repartitioningFinished = false

  def isRepartitioningFinished(): Boolean = _repartitioningFinished

  def finishRepartitioning(): Unit = {
    _repartitioningFinished = true
  }
}
case class WorkerTaskData(info: RepartitioningInfo, scanner: Scanner)

/**
  * Common interface for each repartitioning tracker.
  *
  * Each
  */
private[spark] abstract class RepartitioningTracker(conf: SparkConf)
  extends Logging with RpcEndpoint {
  var master: RpcEndpointRef = _
}

/**
  * Tracks and aggregates histograms of certain tasks of jobs, where dynamic repartitioning
  * is enabled.
  *
  * @todo Currently repartitioning mode is compatible only for one job!
  */
private[spark] class RepartitioningTrackerMaster(override val rpcEnv: RpcEnv,
                                                 conf: SparkConf)
  extends RepartitioningTracker(conf) with SparkListener with ColorfulLogging {
  /**
    * Collection of repartitioning workers. We expect them to register.
    */
  private val workers = mutable.HashMap[String, Worker]()
  /**
    * Local worker in case when running in local mode.
    */
  private var localWorker: Option[RepartitioningTrackerWorker] = None

  /**
    * Final histograms recorded by repartitioning workers.
    * This can be switched with configuration
    * `spark.repartitioning.final-histgorams`. Default value is false.
    */
  private val finalHistograms =
    mutable.HashMap[Int, mutable.HashMap[Long, DataCharacteristics[Any]]]()

  var doneRepartitioning = false

  /**
    * Pending stages to dynamically repartition. These stages are currently
    * running and we're waiting their tasks' histograms to arrive.
    * It also contains repartitioning strategies for stages.
    */
  private val _stageData = mutable.HashMap[Int, MasterStageData]()

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
        }

      /**
        * The case when a worker sends histogram snapshot of a task.
        *
        * We need to identify the stage that this particular task
        * belongs to.
        */
      case ShuffleWriteStatus(stageID, taskID, partitionID,
                              keyHistogram: DataCharacteristics[Any]) =>
        logInfo(s"Received ShuffleWriteStatus message for" +
          s"stage $stageID and task $taskID", "DRCommunication")
        _stageData.get(stageID) match {
          case Some(stageData) =>
            logInfo(s"Received key histogram for stage $stageID" +
              s" task $taskID (with size ${keyHistogram.localValue.size}).",
              "DRCommunication", "DRHistogram")
            logInfo(s"Histogram content is:")
            logInfo(keyHistogram.localValue.map(_.toString).mkString("\n"))
            stageData.strategy.onHistogramArrival(partitionID, keyHistogram)
            context.reply(true)
          case None =>
            logWarning(s"Histograms arrived for invalid stage $stageID.",
              "DRCommunication", "DRHistogram")
            context.reply(false)
        }

      /**
        * Only for debugging purposes. Scans and sends the whole data
        * distribution to the master.
        */
      case FinalHistogram(stageID, taskID, finalHistogram) =>
        finalHistograms.get(stageID) match {
          case Some(stageFinalHistograms) =>
            stageFinalHistograms.get(taskID) match {
              case Some(_) =>
                logWarning(s"Duplicate arrival of final histogram" +
                  s"for stage $stageID and task $taskID.")
              case None => stageFinalHistograms.update(taskID, finalHistogram)
            }
          case None =>
            val map = mutable.HashMap[Long, DataCharacteristics[Any]]()
            map.update(taskID, finalHistogram)
            finalHistograms.update(stageID, map)
        }
        context.reply(true)
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
        val stageID = stageSubmitted.stageInfo.stageId
        logInfo(s"A stage with id $stageID submitted with dinamic repartitioning " +
                s"mode $repartitioningMode", "DRCommunication")
        _stageData.update(stageID,
          new MasterStageData(stageInfo,
                              new Strategy(stageID, stageInfo.numTasks, partitioner.get),
                              repartitioningMode))
        val scanStrategy = new ScanStrategy(stageID, new ThroughputPrototype())
        logInfo(s"Sending repartitioning scan-strategy to each worker for" +
                s"job $stageID", "DRCommunication")
        workers.values.foreach(_.reference.send(scanStrategy))
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.synchronized {
      val stageID = taskEnd.stageId
      if (_stageData.contains(stageID)) {
        if (taskEnd.reason == Success) {
          // Currently we disable repartitioning for a stage, if any of its tasks finish.
          logInfo(s"A task completion detected for stage $stageID." +
            s"Clearing tracking.", "DRCommunication")
          if(!doneRepartitioning) {
            shutDownScanners(stageID)
            if (_stageData(stageID).mode == RepartitioningModes.ONLY_ONCE) {
              doneRepartitioning = true
            }
          }
        } else {
          logWarning(s"Detected completion of a failed task for stage:$stageID!", "DRCommunication")
        }
      } else {
        logWarning(s"Invalid stage of id $stageID detected on task completion!", "DRCommunication")
      }
    }
  }

  def shutDownScanners(stageId: Int): Unit = {
    logInfo(s"Shutting down scanners for stage $stageId.", "DRCommunication")
    workers.values.foreach(_.reference.send(new ShutDownScanners(stageId)))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.synchronized {
      val stageID = stageCompleted.stageInfo.stageId
      workers.values.foreach(_.reference.send(new ClearStageData(stageID)))
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
      _.reference.send(
        new RepartitioningStrategy(stageID, repartitioner, version)))
  }
}

private[spark] class RepartitioningTrackerWorker(override val rpcEnv: RpcEnv,
                                                 conf: SparkConf,
                                                 executorId: String)
  extends RepartitioningTracker(conf) with RpcEndpoint with ColorfulLogging {

  private val stageData = mutable.HashMap[Int, RepartitioningStageData]()
  /**
    * @todo Use this thread pool to instantiate scanners.
    */
  private val threadPool =
    ThreadUtils.newDaemonCachedThreadPool("Executor repartitioning scanner worker")

  rpcEnv.setupEndpoint(RepartitioningTracker.WORKER_ENDPOINT_NAME, this)

  def register(): Unit = {
    logInfo("Registering with Master tracker.", "DRCommunication")
    sendTracker(new Register(executorId, self))
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

            sd.scannedTasks.update(taskID, new WorkerTaskData(repartitioningInfo, scanner))
          }

          logInfo(s"Added TaskContext $taskContext for stage $stageID task $taskID to" +
                  s"scanned tasks on worker $executorId.", "scannedTasks")
          logInfo(s"Scanned tasks after update on worker $executorId, ${sd.scannedTasks}",
                  "scannedTasks")
        case None =>
          logWarning(s"Task with id $taskID arrived for non-registered stage of id $stageID." +
                     s"Doing nothing.", "DRCommunication")
      }
    }
  }

  /**
    * Called by scanners to send histograms through the workers to the master.
    */
  def sendHistogram(stageID: Int, taskID: Long,
                    partitionID: Int,
                    keyHistogram: DataCharacteristics[_]): Unit = {
    logInfo(s"Sending histogram to driver for stage $stageID task $taskID",
      "DRCommunication", "DRHistogram")
    sendTracker(
      new ShuffleWriteStatus(stageID, taskID, partitionID, keyHistogram))
  }

  def sendFinalHistogram(stageID: Int, taskID: Long, finalHistogram: DataCharacteristics[_]):
  Unit = {
    logInfo(s"Sending final histogram to driver for stage $stageID task $taskID.",
            "DRCommunication", "DRHistogram")
    sendTracker(new FinalHistogram(stageID, taskID, finalHistogram))
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
    case ScanStrategy(stageID, scanner) =>
      logInfo(s"Received scan strategy for stage $stageID", "DRCommunication")
      stageData.put(stageID, new RepartitioningStageData(scanner))
    case RepartitioningStrategy(stageID, repartitioner, version) =>
      logInfo(s"Received repartitioning strategy for stage $stageID.",
              "DRCommunication", "DRRepartitioner")
      stageData.get(stageID) match {
        case Some(sd) =>
          sd.partitioner = Some(repartitioner)
          sd.version = Some(version)
          logInfo(s"Scanned tasks before repartitioning on worker $executorId, ${sd.scannedTasks}",
                  "DRRepartitioner")
          logInfo(s"Scanned partitions are" +
                  s" ${sd.scannedTasks.values.map(_.scanner.taskContext.partitionId())}",
                  "DRRepartitioner")
          sd.scannedTasks.values.foreach(wtd => {
            wtd.info.updateRepartitioner(repartitioner, version)
            logInfo(s"Repartitioner set for stage $stageID task ${wtd.info.taskID} on" +
                    s"worker $executorId", "DRRepartitioner")
          })
        case None =>
          logWarning(s"Repartitioner arrived for non-registered stage of id $stageID." +
                     s"Doing nothing.", "DRRepartitioner")
      }
    case ShutDownScanners(stageID) =>
      logInfo(s"Stopping scanners for stage $stageID on executor $executorId.", "DRCommunication")
      stopScanners(stageID)
    case ClearStageData(stageID) =>
      logInfo(s"Clearing stage data for stage $stageID on executor $executorId.", "DRCommunication")
      clearStageData(stageID)
  }

  private def stopScanners(stageID: Int): Unit = {
    stageData.get(stageID) match {
      case Some(sd) =>
        val scannedTasks = sd.scannedTasks
        scannedTasks.keys.foreach(k => scannedTasks.remove(k).foreach({ wtd =>
          wtd.scanner.stop()
          wtd.info.finishTracking()
        }))
        sd.finishRepartitioning()
        sd.version = None
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

  def isStageDone(stageID: Int): Boolean = {
    stageData.get(stageID) match {
      case Some(sd) => sd.isRepartitioningFinished()
      case None => true
    }
  }
}

private[spark] object RepartitioningTracker extends Logging {
  val MASTER_ENDPOINT_NAME = "RepartitioningTrackerMaster"
  val WORKER_ENDPOINT_NAME = "RepartitioningTrackerWorker"

  type Histogram[T] = Map[T, Double]
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
abstract class Scanner() extends Serializable with Runnable with ColorfulLogging {
  var taskContext: TaskContext = _
  var isRunning: Boolean = false

  def stop(): Unit

  def setContext(context: TaskContext): Unit = {
    taskContext = context
  }
}

class ThroughputPrototype extends ScannerPrototype {
  def newInstance(): Scanner = new Throughput()
}

class Throughput() extends Scanner {
  private var lastHistogramHeight: Long = 0
  private val keyHistogramWidth: Int =
    SparkEnv.get.conf.getInt("spark.repartitioning.key-histogram.truncate", 50)

  override def stop(): Unit = {
    isRunning = false
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
    Thread.sleep(SparkEnv.get.conf.getInt("spark.repartitioning.throughput.interval", 1000))
    while (isRunning) {
      taskContext.taskMetrics().shuffleWriteMetrics match {
        case Some(shuffleWriteMetrics) =>
          val histogramValue = shuffleWriteMetrics.dataCharacteristics
          val histogramMeta = histogramValue.getParam
            .asInstanceOf[DataCharacteristicsAccumulatorParam]
          val recordBound =
            SparkEnv.get.conf.getInt("spark.repartitioning.throughput.record-bound", 100)
          val histogramHeightDelta = histogramMeta.recordsPassed - lastHistogramHeight
          if (histogramMeta.width == 0) {
            logInfo(s"Histogram is empty for task ${taskContext.taskAttemptId()}. " +
                    s"Doing Nothing.", "DRHistogram")
          } else if (recordBound > histogramHeightDelta) {
            logInfo(s"Not enough records ($histogramHeightDelta) " +
                    s"processed to send the histogram to the driver.", "DRHistogram")
          } else {
            lastHistogramHeight = histogramMeta.recordsPassed
            SparkEnv.get.repartitioningWorker()
              .sendHistogram(
                taskContext.stageId(),
                taskContext.taskAttemptId(),
                taskContext.partitionId(),
                histogramValue)
          }
        case None =>
          logWarning(s"ShuffleWriteMetrics not found for stage ${taskContext.stageId()} " +
                     s"task ${taskContext.taskAttemptId()}.", "DRHistogram")
      }
      Thread.sleep(SparkEnv.get.conf.getInt("spark.repartitioning.throughput.interval", 1000))
    }
    logInfo(s"Scanner is finishing for stage ${taskContext.stageId()} task" +
            s" ${taskContext.taskAttemptId()}.", "default", "strongBlue")
  }
}

/**
  * A decider strategy, that continuously receives histograms from the physical tasks
  * and decides when and how to repartition a certain stage.
  */
abstract class Decider(stageID: Int, numberOfTasks: Int) {
  def onHistogramArrival(partitionID: Int,
                         keyHistogram: DataCharacteristics[Any]): Unit

  protected def decide(): Boolean

  protected def repartition(globalHistogram: Seq[(Any, Double)]): Unit
}

/**
  * A simple strategy to decide when and how to repartition a stage.
  */
class Strategy(stageID: Int,
               numberOfTasks: Int,
               partitioner: Partitioner)
  extends Decider(stageID, numberOfTasks) with ColorfulLogging with Serializable {

  private val numPartitions = partitioner.numPartitions
  var repartitionCount = 0
  private val histograms = mutable.HashMap[Int, DataCharacteristics[Any]]()
  private val broadcastHistory = mutable.ArrayBuffer[Partitioner]()
  private var currentVersion = 0
  private var cut = SparkEnv.get.conf.getInt("spark.repartitioning.default-cut", 2)
  private val sCut = SparkEnv.get.conf.getInt("spark.repartitioning.default-scut", 0)

  private val desiredNumberOfHistograms = numberOfTasks

  /**
    * @todo Remove these assertions in the future.
    */
  assert(numPartitions > 0)
  assert(cut >= -1)
  assert(cut <= numPartitions)
  assert(sCut >= 0)
  println("scut: " + sCut + ", cut: " + cut)
  assert(sCut <= cut)
  assert(sCut <= numPartitions - 1)

  /**
    * Called by the RepartitioningTrackerMaster if new histogram arrives
    * for this particular job's strategy.
    */
  override def onHistogramArrival(partitionID: Int,
                                  keyHistogram: DataCharacteristics[Any]): Unit = {
    this.synchronized {
      val histogramMeta = keyHistogram.getParam.asInstanceOf[DataCharacteristicsAccumulatorParam]
      if (histogramMeta.version == currentVersion) {
        logInfo(s"Recording histogram arrival for partition $partitionID.",
                "DRCommunication", "DRHistogram")
        logInfo(s"Updating histogram for partition $partitionID.", "DRHistogram")
        histograms.update(partitionID, keyHistogram)
        if (!SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", true) ||
          repartitionCount == 0) {
          if (decide()) {
            repartitionCount += 1
            if (SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", true)) {
              SparkEnv.get.repartitioningTracker
                .asInstanceOf[RepartitioningTrackerMaster]
                .shutDownScanners(stageID)
            }
          }
        }
      } else if (histogramMeta.version < currentVersion) {
        logInfo(s"Recording outdated histogram arrival for partition $partitionID." +
                s"Doing nothing.", "DRCommunication", "DRHistogram")
      } else {
        logInfo(s"Recording histogram arrival from a future step for" +
                s"partition $partitionID. Doing nothing.", "DRCommunication", "DRHistogram")
      }
    }
  }

  /**
    * Decides if repartitioning is needed. If so, constructs a new
    * partitioning function and sends the strategy to each worker.
    * It asks the RepartitioningTrackerMaster to broadcast the new
    * strategy to workers.
    */
  override protected def decide(): Boolean = {
    logInfo(s"Deciding if need any repartitioning now.", "DRRepartitioner")

    val globalHistogram =
      histograms.values.map(_.localValue)
        .reduce(DataCharacteristicsAccumulatorParam.merge[Any, Double](0.0)(
          (a: Double, b: Double) => a + b)
        )
        .toSeq.sortBy(-_._2).take(numPartitions)

    if (histograms.size >=
      SparkEnv.get.conf.getInt("spark.repartitioning.histogram-threshold", 2)) {
      repartition(globalHistogram)
      true
    } else {
      false
    }
  }

  override protected def repartition(globalHistogram: Seq[(Any, Double)]): Unit = {
    val sortedNormedHistogram = globalHistogram.map(r =>
      (r._1, r._2, r._2.toDouble / globalHistogram.map(_._2).sum))
    logInfo(
      sortedNormedHistogram.foldLeft(
        s"Global histogram for repartitioning " +
          s"step $repartitionCount:\n")((x, y) =>
        x + s"\t${y._1}\t->\t${y._2}\t${y._3}\n"), "DRRepartitioner")

    val heaviest = sortedNormedHistogram.map{
      triple => (triple._1, triple._3)
    }.take(cut)
    val highestValues = heaviest.map(_._2).toArray
    val heaviestKeys = heaviest.map(_._1).toArray
//    cut = Math.min(cut, highestValues.length)
    val partitioningInfo = getPartitioningInfo(highestValues)

    val repartitioner = new KeyIsolationPartitioner(
      partitioningInfo,
      heaviestKeys,
      getWeightedHashPartitioner(highestValues, partitioningInfo)
    )

    logDebug("Partitioner created, simulating run with global histogram.")
    logDebug(heaviest.toString())
    heaviestKeys.foreach {
      key => logInfo(s"Key $key went to ${repartitioner.getPartition(key)}.")
    }

    logInfo(s"Decided to repartition stage $stageID.", "DRRepartitioner")
    currentVersion += 1

    logInfo(s"Sending repartitioning strategy.", "DRCommunication", "DRRepartitioner")

    SparkEnv.get.repartitioningTracker
      .asInstanceOf[RepartitioningTrackerMaster]
      .broadcastRepartitioningStrategy(stageID, repartitioner, currentVersion)
    broadcastHistory += repartitioner

    // Histogram is not going to be valid while using another Partitioner.
    histograms.clear()
    logInfo(s"Version of histograms pushed up for stage $stageID", "DRHistogram")
  }

  def getPartitioningInfo(highestValues: Array[Double]): PartitioningInfo = {
    var remainder = 1.0d
    var level = 1.0d / numPartitions
    var singleKeysCut = 0

    val currentCut = Math.min(cut, highestValues.length)

    def countLevel(i: Int): Unit = {
      if (i < currentCut && level <= highestValues(i)) {
        remainder -= highestValues(i)
        if (i < numPartitions - 1) level = remainder / (numPartitions - 1 - i) else level = 0.0d
        singleKeysCut += 1
        countLevel(i + 1)
      }
    }
    countLevel(0)

    if(sCut > 0) {
      singleKeysCut = Math.max(singleKeysCut, sCut)
      remainder = 1.0d - highestValues.take(singleKeysCut).sum
      level = remainder / (numPartitions - singleKeysCut)
    }

    logInfo(s"Repartitioning parameters: numPartitions=$numPartitions, cut=$currentCut," +
              s"sCut=$singleKeysCut, level=$level, remainder=$remainder," +
              s"block=${(numPartitions - currentCut) * level}, maxKey=${highestValues.head}",
            "DRRepartitioner")

    new PartitioningInfo(numPartitions, currentCut, singleKeysCut, level)
  }

  def getWeightedHashPartitioner(highestValues: Array[Double],
                                 partitioningInfo: PartitioningInfo):
  WeightedHashPartitioner = {
    val cut = partitioningInfo.cut
    val sCut = partitioningInfo.sCut
    val level = partitioningInfo.level

    new WeightedHashPartitioner(
      Array.tabulate[Double]
        (cut - sCut)
        (i => level - highestValues(cut - i - 1)), partitioningInfo, (key: Any) =>
        (MurmurHash3.stringHash((key.hashCode + 123456791).toString).toDouble
          / Int.MaxValue + 1) / 2)
  }
}

class Worker(val executorID: String,
             val reference: RpcEndpointRef)

case class PartitioningInfo(partitions: Int, cut: Int, sCut: Int, level: Double)
package org.apache.spark.repartitioning

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.repartitioning.core._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._

/**
  * Tracks and aggregates histograms of certain tasks of jobs, where dynamic repartitioning
  * is enabled.
  *
  * @todo Currently repartitioning mode is compatible only for one job!
  */
private[spark] class RepartitioningTrackerMaster(
  val rpcEnv: RpcEnv,
  conf: SparkConf)(implicit ev1: ScannerFactory[Throughput])
extends core.RepartitioningTrackerMaster[
  RpcEndpointRef, RpcCallContext, TaskContext, TaskMetrics, RDD[_]]()(ev1)
with RpcEndpoint {
  class Listener extends SparkListener {
    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      totalSlots.addAndGet(executorAdded.executorInfo.totalCores)
      logInfo(s"Executor added. Total cores is ${totalSlots.intValue()}.")
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
      totalSlots.addAndGet(-executorRemoved.executorInfo.totalCores)
      logInfo(s"Executor removed. Total cores is ${totalSlots.intValue()}.")
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
      val stageInfo = stageSubmitted.stageInfo
      whenStageSubmitted(
        stageInfo.jobId,
        stageInfo.stageId,
        stageInfo.attemptId,
        if (stageInfo.isInstanceOf[ResultStageInfo] || stageInfo.partitioner.isEmpty) {
          RepartitioningModes.OFF
        } else {
          configuredRPMode
        })
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      whenTaskEnd(taskEnd.stageId, taskEnd.reason match {
        case Success => TaskEndReason.Success
        case _ => TaskEndReason.Failed
      })
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      whenStageCompleted(
        stageCompleted.stageInfo.stageId,
        if (stageCompleted.stageInfo.getStatusString == "succeeded") {
          StageEndReason.Success
        } else {
          StageEndReason.Fail
        }
      )
    }
  }

  val eventListener = new Listener

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
    * This is for the RpcEndpoint.
    */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    componentReceiveAndReply(context)
  }

  override def scannerFactory(): ScannerFactory[core.Throughput[TaskContext, TaskMetrics]] = {
    implicitly[ScannerFactory[Throughput]]
  }
}
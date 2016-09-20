package org.apache.spark.repartitioning

import org.apache.spark.Partitioner
import org.apache.spark.repartitioning.core.{ScannerFactory, TaskContextInterface, TaskMetricsInterface, WorkerTaskData}

import scala.collection.mutable

case class RepartitioningStageData[TaskContext <: TaskContextInterface[TaskMetrics],
                                   TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  var scanner: core.Scanner[TaskContext, TaskMetrics],
  var scannedTasks: Option[mutable.Map[Long, WorkerTaskData[TaskContext, TaskMetrics]]] =
  Some(mutable.Map[Long, WorkerTaskData[TaskContext, TaskMetrics]]()),
  var partitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0)) {

  var _repartitioningFinished = false

  def isRepartitioningFinished: Boolean = _repartitioningFinished

  def finishRepartitioning(): Unit = {
    _repartitioningFinished = true
    scannedTasks = None
    version = None
  }
}
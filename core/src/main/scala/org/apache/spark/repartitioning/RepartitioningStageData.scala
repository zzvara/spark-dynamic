package org.apache.spark.repartitioning

import org.apache.spark.Partitioner

import scala.collection.mutable

case class RepartitioningStageData(
  var scannerPrototype: ScannerPrototype,
  var scannedTasks: Option[mutable.Map[Long, WorkerTaskData]] =
    Some(mutable.Map[Long, WorkerTaskData]()),
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
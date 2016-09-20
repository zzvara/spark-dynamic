package org.apache.spark.repartitioning.core

import org.apache.spark.executor.RepartitioningInfo
import org.apache.spark.util.DataCharacteristicsAccumulator

abstract class TaskMetricsInterface[TaskMetrics <: TaskMetricsInterface[TaskMetrics]] {
  var repartitioningInfo: Option[RepartitioningInfo[TaskMetrics]]
  def writeCharacteristics: DataCharacteristicsAccumulator
}

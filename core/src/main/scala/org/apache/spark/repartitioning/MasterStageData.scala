package org.apache.spark.repartitioning

import org.apache.spark.repartitioning.core.messaging.StandaloneStrategy
import org.apache.spark.repartitioning.core.{RepartitioningModes, Strategy, TaskContextInterface, TaskMetricsInterface}

case class MasterStageData[TaskContext <: TaskContextInterface[TaskMetrics],
                           TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  stageID: Int,
  strategy: Strategy,
  mode: RepartitioningModes.Value,
  scanStrategy: StandaloneStrategy[TaskContext, TaskMetrics])

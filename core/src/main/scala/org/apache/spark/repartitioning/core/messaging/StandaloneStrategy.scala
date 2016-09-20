package org.apache.spark.repartitioning.core.messaging

import org.apache.spark.repartitioning.core
import org.apache.spark.repartitioning.core.{TaskContextInterface, TaskMetricsInterface}

/**
  * Scan strategy message sent to workers.
  */
case class StandaloneStrategy[TaskContext <: TaskContextInterface[TaskMetrics],
                              TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  stageID: Int,
  scanner: core.Scanner[TaskContext, TaskMetrics])
extends ScanStrategy

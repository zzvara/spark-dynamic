package org.apache.spark.repartitioning.core

import org.apache.spark.internal.ColorfulLogging
import org.apache.spark.util.DataCharacteristicsAccumulator

/**
  * Decides when to send the histogram to the master from the workers.
  *
  * This strategy should run somewhere near the TaskMetrics and should decide
  * based on many factors when to send the histogram to the master.
  * Also, it should declare the sampling method.
  */
abstract class Scanner[
  TaskContext <: TaskContextInterface[TaskMetrics],
  TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  val totalSlots: Int,
  histogramDrop: (Int, Long, Int, DataCharacteristicsAccumulator) => Unit)
extends Serializable with Runnable with ColorfulLogging {
  var taskContext: TaskContext = _

  def setContext(context: TaskContext): Unit = {
    taskContext = context
  }

  var isRunning: Boolean = false

  def stop(): Unit = {
    isRunning = false
  }
}

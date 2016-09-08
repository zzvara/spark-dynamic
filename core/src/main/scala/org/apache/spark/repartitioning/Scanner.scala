package org.apache.spark.repartitioning

import org.apache.spark.TaskContext
import org.apache.spark.internal.ColorfulLogging

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

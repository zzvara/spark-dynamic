package org.apache.spark.repartitioning

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.repartitioning.core.ScannerFactory
import org.apache.spark.util.{DataCharacteristicsAccumulator, TaskCompletionListener}

class Throughput(totalSlots: Int,
                 histogramDrop: (Int, Long, Int, DataCharacteristicsAccumulator) => Unit)
extends core.Throughput[TaskContext, TaskMetrics](totalSlots, histogramDrop) {
  override def whenStarted(): Unit = {
    taskContext.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        whenTaskCompleted(context)
      }
    })
  }
}

object Throughput {
  implicit object Factory extends ScannerFactory[Throughput] {
    override def apply(totalSlots: Int,
                       histogramDrop: (Int, Long, Int, DataCharacteristicsAccumulator) => Unit)
    : Throughput = new Throughput(totalSlots, histogramDrop)
  }
}
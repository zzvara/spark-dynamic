package org.apache.spark.repartitioning

import hu.sztaki.drc
import hu.sztaki.drc.{Sampler, ScannerFactory}
import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.TaskCompletionListener

class Throughput(totalSlots: Int,
                 histogramDrop: (Int, Long, Int, Sampler) => Unit)
extends drc.Throughput[TaskContext, TaskMetrics](totalSlots, histogramDrop) {
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
                       histogramDrop: (Int, Long, Int, Sampler) => Unit)
    : Throughput = new Throughput(totalSlots, histogramDrop)
  }
}
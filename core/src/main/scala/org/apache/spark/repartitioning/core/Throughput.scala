package org.apache.spark.repartitioning.core

import org.apache.spark.repartitioning.core
import org.apache.spark.util.DataCharacteristicsAccumulator

abstract class Throughput[
  TaskContext <: TaskContextInterface[TaskMetrics],
  TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  totalSlots: Int,
  histogramDrop: (Int, Long, Int, DataCharacteristicsAccumulator) => Unit)
extends core.Scanner[TaskContext, TaskMetrics](totalSlots, histogramDrop) {
  private var lastHistogramHeight: Long = 0
  private val keyHistogramWidth: Int =
    Configuration.internal().getInt("spark.repartitioning.key-histogram.truncate")

  /**
    * Reconfigures the data-characteristics sampling by updating the total slots available.
    * @param dataCharacteristics Data characteristics shuffle write metrics.
    */
  def updateTotalSlots(dataCharacteristics: DataCharacteristicsAccumulator): Unit = {
    logInfo("Updating number of total slots.")
    dataCharacteristics.updateTotalSlots(totalSlots)
  }

  def whenTaskCompleted(context: TaskContext): Unit = {
    logInfo(s"Detected completion for stage ${taskContext.stageId()} task" +
      s" ${taskContext.attemptNumber()}.", "DRCommunication")
    isRunning = false
  }

  def whenStarted(): Unit = {}

  override def run(): Unit = {
    logInfo(s"Running scanner for stage ${taskContext.stageId()} task" +
      s" ${taskContext.attemptNumber()}.", "default", "strongBlue")
    require(taskContext != null, "Scanner needs to have a valid task context!")
    isRunning = true
    whenStarted()

    updateTotalSlots(taskContext.taskMetrics().writeCharacteristics)

    Thread.sleep(Configuration.internal().getInt("spark.repartitioning.throughput.interval"))
    while (isRunning) {
      val dataCharacteristics = taskContext.taskMetrics().writeCharacteristics
      val recordBound =
        Configuration.internal().getInt("spark.repartitioning.throughput.record-bound")
      val histogramHeightDelta = dataCharacteristics.recordsPassed - lastHistogramHeight
      if (dataCharacteristics.width == 0) {
        logInfo(s"Histogram is empty for task ${taskContext.attemptNumber()}. " +
                s"Doing Nothing.", "DRHistogram")
      } else if (recordBound > histogramHeightDelta) {
        logInfo(s"Not enough records ($histogramHeightDelta) " +
                s"processed to send the histogram to the driver.", "DRHistogram")
      } else {
        lastHistogramHeight = dataCharacteristics.recordsPassed
        histogramDrop(
          taskContext.stageId(),
          taskContext.attemptNumber(),
          taskContext.partitionID(),
          dataCharacteristics)
      }
      Thread.sleep(Configuration.internal().getInt("spark.repartitioning.throughput.interval"))
      updateTotalSlots(taskContext.taskMetrics().writeCharacteristics)
    }
    logInfo(s"Scanner is finishing for stage ${taskContext.stageId()} task" +
            s" ${taskContext.attemptNumber()}.", "default", "strongBlue")
  }
}

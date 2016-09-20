package org.apache.spark.repartitioning

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.repartitioning.core.{Configuration, ScannerFactory}
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{SparkEnv, TaskContext}

class Throughput(override val totalSlots: Int) extends Scanner(totalSlots) {
  private var lastHistogramHeight: Long = 0
  private val keyHistogramWidth: Int =
    Configuration.internal().getInt("spark.repartitioning.key-histogram.truncate")

  /**
    * Reconfigures the data-characteristics sampling by updating the total slots available.
    * @param shuffleWriteMetrics Spark's shuffle write metrics.
    */
  def updateTotalSlots(shuffleWriteMetrics: ShuffleWriteMetrics): Unit = {
    logInfo("Updating number of total slots.")
    shuffleWriteMetrics.dataCharacteristics.updateTotalSlots(totalSlots)
  }

  override def run(): Unit = {
    logInfo(s"Running scanner for stage ${taskContext.stageId()} task" +
            s" ${taskContext.taskAttemptId()}.", "default", "strongBlue")
    require(taskContext != null, "Scanner needs to have a valid task context!")
    isRunning = true
    taskContext.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        logInfo(s"Detected completion for stage ${taskContext.stageId()} task" +
                s" ${taskContext.taskAttemptId()}.", "DRCommunication")
        isRunning = false
      }
    })

    updateTotalSlots(taskContext.taskMetrics().shuffleWriteMetrics)

    Thread.sleep(Configuration.internal().getInt("spark.repartitioning.throughput.interval"))
    while (isRunning) {
      val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
      val dataCharacteristics = shuffleWriteMetrics.dataCharacteristics
      val recordBound =
        Configuration.internal().getInt("spark.repartitioning.throughput.record-bound")
      val histogramHeightDelta = dataCharacteristics.recordsPassed - lastHistogramHeight
      if (dataCharacteristics.width == 0) {
        logInfo(s"Histogram is empty for task ${taskContext.taskAttemptId()}. " +
                s"Doing Nothing.", "DRHistogram")
      } else if (recordBound > histogramHeightDelta) {
        logInfo(s"Not enough records ($histogramHeightDelta) " +
                s"processed to send the histogram to the driver.", "DRHistogram")
      } else {
        lastHistogramHeight = dataCharacteristics.recordsPassed
        SparkEnv.get.repartitioningWorker().get
          .sendHistogram(
            taskContext.stageId(),
            taskContext.taskAttemptId(),
            taskContext.partitionId(),
            dataCharacteristics)
      }
      Thread.sleep(Configuration.internal().getInt("spark.repartitioning.throughput.interval"))
      updateTotalSlots(taskContext.taskMetrics().shuffleWriteMetrics)
    }
    logInfo(s"Scanner is finishing for stage ${taskContext.stageId()} task" +
            s" ${taskContext.taskAttemptId()}.", "default", "strongBlue")
  }
}

object Throughput {
  implicit object Factory extends ScannerFactory[Throughput] {
    override def apply(totalSlots: Int): Throughput = new Throughput(totalSlots)
  }
}
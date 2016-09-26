package org.apache.spark.repartitioning.core

abstract class TaskContextInterface[TaskMetrics <: TaskMetricsInterface[TaskMetrics]] {
  def taskMetrics(): TaskMetrics
  def partitionID(): Int
  def attemptId(): Int
  def stageId(): Int
  def attemptNumber(): Int
}

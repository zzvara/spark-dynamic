package org.apache.spark.repartitioning.core

/**
  * Represents a repartitioning tracker worker as a reference.
  * @param executorID ID of the Spark executor or Flink task manager.
  * @param reference Reference object used to send message to the worker.
  * @tparam ComponentReference Reference object type to send message to the worker.
  */
class WorkerReference[ComponentReference](
  val executorID: String,
  val reference: ComponentReference)

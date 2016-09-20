package org.apache.spark.repartitioning.core.messaging

import org.apache.spark.Partitioner

/**
  * Repartitioning strategy message sent to workers.
  */
case class RepartitioningStrategy(
  stageID: Int,
  repartitioner: Partitioner,
  version: Int) extends RepartitioningTrackerMessage

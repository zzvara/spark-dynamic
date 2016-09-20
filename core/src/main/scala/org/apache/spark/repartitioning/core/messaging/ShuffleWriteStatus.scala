package org.apache.spark.repartitioning.core.messaging

import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.reflect.ClassTag

/**
  * Shuffle write status message.
  */
case class ShuffleWriteStatus[T: ClassTag](
  stageID: Int,
  taskID: Long,
  partitionID: Int,
  keyHistogram: DataCharacteristicsAccumulator) extends RepartitioningTrackerMessage

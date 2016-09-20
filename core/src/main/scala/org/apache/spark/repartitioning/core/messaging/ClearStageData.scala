package org.apache.spark.repartitioning.core.messaging

/**
  * Scan strategy message sent to workers.
  */
case class ClearStageData(stageID: Int) extends RepartitioningTrackerMessage

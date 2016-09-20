package org.apache.spark.repartitioning.core.messaging

/**
  * Scan strategy message sent to workers.
  */
case class ShutDownScanners(stageID: Int)
  extends RepartitioningTrackerMessage

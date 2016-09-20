package org.apache.spark.repartitioning.core.messaging

case class ScanStrategies(scanStrategies: List[ScanStrategy])
  extends RepartitioningTrackerMessage

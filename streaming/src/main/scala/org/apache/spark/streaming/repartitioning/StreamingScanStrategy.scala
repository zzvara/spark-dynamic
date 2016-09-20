package org.apache.spark.streaming.repartitioning

import org.apache.spark.repartitioning.core.messaging.ScanStrategy
import org.apache.spark.streaming.repartitioning.decider.StreamingDecider

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class StreamingScanStrategy(
  streamID: Int,
  strategy: StreamingDecider,
  parentStreams: collection.immutable.Set[Int])
extends ScanStrategy

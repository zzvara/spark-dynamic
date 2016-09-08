package org.apache.spark.streaming.repartitioning

import org.apache.spark.repartitioning.ScanStrategy

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class StreamingScanStrategy(
  streamID: Int,
  strategy: StreamingStrategy,
  parentStreams: collection.immutable.Set[Int])
extends ScanStrategy

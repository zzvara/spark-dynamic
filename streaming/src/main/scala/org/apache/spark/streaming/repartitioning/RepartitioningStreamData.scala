package org.apache.spark.streaming.repartitioning

import org.apache.spark.streaming.repartitioning.decider.StreamingDecider

case class RepartitioningStreamData(
  streamID: Int,
  strategy: StreamingDecider,
  parentStreams: collection.immutable.Set[Int])

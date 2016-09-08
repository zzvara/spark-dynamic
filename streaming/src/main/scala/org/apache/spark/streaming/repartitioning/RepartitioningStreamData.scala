package org.apache.spark.streaming.repartitioning

case class RepartitioningStreamData(
  streamID: Int,
  strategy: StreamingStrategy,
  parentStreams: collection.immutable.Set[Int])

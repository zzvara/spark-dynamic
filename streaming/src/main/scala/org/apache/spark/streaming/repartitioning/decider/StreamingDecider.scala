package org.apache.spark.streaming.repartitioning.decider

import org.apache.spark.repartitioning.core.Decider
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.Stream

abstract class StreamingDecider(
  streamID: Int,
  stream: Stream,
  val perBatchSamplingRate: Int = 1,
  resourceStateHandler: Option[() => Int] = None)
extends Decider(streamID, resourceStateHandler) {
  def zeroTime: Time = stream.time
}

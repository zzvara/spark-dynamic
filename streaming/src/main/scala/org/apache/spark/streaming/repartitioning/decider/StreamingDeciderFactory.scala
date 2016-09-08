package org.apache.spark.streaming.repartitioning.decider

import org.apache.spark.streaming.dstream.Stream

abstract class StreamingDeciderFactory {
  def apply(streamID: Int,
            stream: Stream,
            perBatchSamplingRate: Int = 1,
            resourceStateHandler: Option[() => Int] = None): StreamingDecider
}

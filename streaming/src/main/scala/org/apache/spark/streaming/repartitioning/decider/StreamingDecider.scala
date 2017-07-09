
package org.apache.spark.streaming.repartitioning.decider

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.Stream

abstract class StreamingDecider(
  streamID: Int,
  stream: Stream,
  override val perBatchSamplingRate: Int = 1,
  resourceStateHandler: Option[() => Int] = None)
extends hu.sztaki.drc.StreamingDecider[Stream](
  streamID, stream, perBatchSamplingRate, resourceStateHandler) {
  def zeroTime: Long = stream.time
  def onPartitionMetricsArrival(partitionID: Int, recordsRead: Long): Unit
}

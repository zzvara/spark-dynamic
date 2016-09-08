package org.apache.spark.streaming.repartitioning

import org.apache.spark.streaming.dstream.Stream

case class MasterJobData(
  jobID: Int,
  stream: Stream)

package org.apache.spark.repartitioning

import org.apache.spark.SparkConf
import org.apache.spark.internal.{ColorfulLogging, Logging}
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef}
import org.apache.spark.scheduler.SparkListener

import scala.collection.mutable

/**
  * Common interface for each repartitioning tracker.
  */
private[spark] abstract class RepartitioningTracker(conf: SparkConf)
  extends SparkListener with ColorfulLogging with RpcEndpoint {
  var master: RpcEndpointRef = _
}

private[spark] object RepartitioningTracker extends Logging {
  val MASTER_ENDPOINT_NAME = "RepartitioningTrackerMaster"
  val WORKER_ENDPOINT_NAME = "RepartitioningTrackerWorker"

  type Histogram[T] = mutable.Map[T, Double]
}
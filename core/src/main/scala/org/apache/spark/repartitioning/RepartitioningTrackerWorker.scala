package org.apache.spark.repartitioning

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.repartitioning.core.RepartitioningTracker
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}

private[spark] class RepartitioningTrackerWorker(val rpcEnv: RpcEnv,
                                                 conf: SparkConf,
                                                 executorID: String)
extends core.RepartitioningTrackerWorker[
  RpcEndpointRef, RpcEndpointRef, TaskContext, TaskMetrics, RDD[_]](
  executorID)
with RpcEndpoint {
  rpcEnv.setupEndpoint(RepartitioningTracker.WORKER_ENDPOINT_NAME, this)

  override def selfReference: RpcEndpointRef = self

  override def receive: PartialFunction[Any, Unit] = componentReceive
}

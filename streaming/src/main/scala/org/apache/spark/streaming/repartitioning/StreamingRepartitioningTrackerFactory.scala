package org.apache.spark.streaming.repartitioning

import org.apache.spark.SparkConf
import org.apache.spark.repartitioning.{RepartitioningTrackerFactory, RepartitioningTrackerMaster, RepartitioningTrackerWorker}
import org.apache.spark.rpc.RpcEnv

class StreamingRepartitioningTrackerFactory extends RepartitioningTrackerFactory {
  def createMaster(rpcEnv: RpcEnv, conf: SparkConf): RepartitioningTrackerMaster = {
    new StreamingRepartitioningTrackerMaster(rpcEnv, conf)
  }
  def createWorker(rpcEnv: RpcEnv, conf: SparkConf,
                   executorId: String): RepartitioningTrackerWorker = {
    new StreamingRepartitioningTrackerWorker(rpcEnv, conf, executorId)
  }
}

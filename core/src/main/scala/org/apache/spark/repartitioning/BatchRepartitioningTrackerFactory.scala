package org.apache.spark.repartitioning

import org.apache.spark.SparkConf
import org.apache.spark.rpc.RpcEnv

/**
  * Factory to create a core (batch) repartitioning tracker.
  */
class BatchRepartitioningTrackerFactory extends RepartitioningTrackerFactory {
  def createMaster(rpcEnv: RpcEnv, conf: SparkConf): RepartitioningTrackerMaster = {
    new RepartitioningTrackerMaster(rpcEnv, conf)
  }
  def createWorker(rpcEnv: RpcEnv, conf: SparkConf,
                   executorId: String): RepartitioningTrackerWorker = {
    new RepartitioningTrackerWorker(rpcEnv, conf, executorId)
  }
}

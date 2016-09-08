package org.apache.spark.repartitioning

import org.apache.spark.SparkConf
import org.apache.spark.rpc.RpcEnv

abstract class RepartitioningTrackerFactory {
  def createMaster(rpcEnv: RpcEnv, conf: SparkConf): RepartitioningTrackerMaster
  def createWorker(rpcEnv: RpcEnv, conf: SparkConf,
                   executorId: String): RepartitioningTrackerWorker
}

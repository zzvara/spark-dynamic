package org.apache.spark.repartitioning.core

abstract class StrategyFactory[+S <: Strategy] extends Serializable {
  def apply(stageID: Int, attemptID: Int, numPartitions: Int,
            resourceStateHandler: Option[() => Int] = None): S
}

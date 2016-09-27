package org.apache.spark.repartitioning

import org.apache.spark.repartitioning.core.{Strategy, StrategyFactory}
import org.apache.spark.{Partitioner, SparkContext, SparkEnv}

class NaivBatchStrategy(
  stageID: Int,
  attemptID: Int,
  numPartitions: Int,
  resourceStateHandler: Option[() => Int] = None)
extends Strategy(stageID, attemptID, numPartitions, resourceStateHandler) {

  override def getTrackerMaster: core.RepartitioningTrackerMaster[_, _, _, _, _] =
    SparkEnv.get.repartitioningTracker.asInstanceOf[core.RepartitioningTrackerMaster[_, _, _, _, _]]

  /**
    * In addition to core functionality defined in core.Strategy, this decider
    * resets the partitioner in the DAG scheduler as well.
    * @param newPartitioner Partitioner to reset to.
    */
  override protected def resetPartitioners(newPartitioner: Partitioner): Unit = {
    SparkContext.getOrCreate().dagScheduler
      .refineChildrenStages(stageID, newPartitioner.numPartitions)
    super.resetPartitioners(newPartitioner)
  }
}

object NaivBatchStrategy {
  implicit object NaivBatchStrategyFactory extends StrategyFactory[Strategy] {
    override def apply(stageID: Int, attemptID: Int, numPartitions: Int,
                       resourceStateHandler: Option[() => Int] = None): NaivBatchStrategy = {
      new NaivBatchStrategy(stageID, attemptID, numPartitions, resourceStateHandler)
    }
  }
}
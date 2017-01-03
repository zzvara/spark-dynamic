package org.apache.spark.repartitioning

import hu.sztaki.drc.{DeciderStrategy, StrategyFactory, partitioner}
import org.apache.spark.{Partitioner, SparkContext, SparkEnv}

class NaivBatchStrategy(
  stageID: Int,
  attemptID: Int,
  numPartitions: Int,
  resourceStateHandler: Option[() => Int] = None)
extends DeciderStrategy(stageID, attemptID, numPartitions, resourceStateHandler) {

  override def getTrackerMaster: hu.sztaki.drc.component.RepartitioningTrackerMaster[_, _, _, _, _] =
    SparkEnv.get.repartitioningTracker.asInstanceOf[hu.sztaki.drc.component.RepartitioningTrackerMaster[_, _, _, _, _]]

  /**
    * In addition to core functionality defined in core.Strategy, this decider
    * resets the partitioner in the DAG scheduler as well.
    * @param newPartitioner Partitioner to reset to.
    */
  override protected def resetPartitioners(newPartitioner: partitioner.Partitioner): Unit = {
    SparkContext.getOrCreate().dagScheduler
      .refineChildrenStages(stageID, newPartitioner.size)
    super.resetPartitioners(newPartitioner)
  }
}

object NaivBatchStrategy {
  implicit object NaivBatchStrategyFactory extends StrategyFactory[DeciderStrategy] {
    override def apply(stageID: Int, attemptID: Int, numPartitions: Int,
                       resourceStateHandler: Option[() => Int] = None): NaivBatchStrategy = {
      new NaivBatchStrategy(stageID, attemptID, numPartitions, resourceStateHandler)
    }
  }
}
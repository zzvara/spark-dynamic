package org.apache.spark.repartitioning

import org.apache.spark.util.DataCharacteristicsAccumulator
import org.apache.spark.{Partitioner, SparkContext, SparkEnv}

/**
  * A simple strategy to decide when and how to repartition a stage.
  */
class Strategy(stageID: Int,
  attemptID: Int,
  var numPartitions: Int)
extends Decider(stageID) {

  /**
    * Called by the RepartitioningTrackerMaster if new histogram arrives
    * for this particular job's strategy.
    */
  override def onHistogramArrival(partitionID: Int,
    keyHistogram: DataCharacteristicsAccumulator): Unit = {
    this.synchronized {
      if (keyHistogram.version == currentVersion) {
        logInfo(s"Recording histogram arrival for partition $partitionID.",
          "DRCommunication", "DRHistogram")
        if (!SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", true) ||
          repartitionCount == 0) {
          logInfo(s"Updating histogram for partition $partitionID.", "DRHistogram")
          histograms.update(partitionID, keyHistogram)
          if (repartition()) {
            repartitionCount += 1
            if (SparkEnv.get.conf.getBoolean("spark.repartitioning.only.once", true)) {
              /*
              SparkEnv.get.repartitioningTracker
                .asInstanceOf[RepartitioningTrackerMaster]
                .shutDownScanners(stageID)
                */
            }
          }
        }
      } else if (keyHistogram.version < currentVersion) {
        logInfo(s"Recording outdated histogram arrival for partition $partitionID. " +
          s"Doing nothing.", "DRCommunication", "DRHistogram")
      } else {
        logInfo(s"Recording histogram arrival from a future step for " +
          s"partition $partitionID. Doing nothing.", "DRCommunication", "DRHistogram")
      }
    }
  }

  override protected def clearHistograms(): Unit = {
    // Histogram is not going to be valid while using another Partitioner.
    histograms.clear()
  }

  override protected def preDecide(): Boolean = {
    histograms.size >=
      SparkEnv.get.conf.getInt("spark.repartitioning.histogram-threshold", 2)
  }

  override protected def decideAndValidate(globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    isValidHistogram(globalHistogram)
  }

  override protected def resetPartitioners(newPartitioner: Partitioner): Unit = {
    SparkContext.getOrCreate().dagScheduler.refineChildrenStages(stageID, newPartitioner.numPartitions)
    SparkEnv.get.repartitioningTracker.get
      .asInstanceOf[RepartitioningTrackerMaster]
      .broadcastRepartitioningStrategy(stageID, newPartitioner, currentVersion)
    broadcastHistory += newPartitioner
    logInfo(s"Version of histograms pushed up for stage $stageID", "DRHistogram")
    clearHistograms()
  }
}

package org.apache.spark.repartitioning.core

import org.apache.spark.internal.{ColorfulLogging, Logging}

/**
  * Common interface for each repartitioning tracker.
  */
abstract class RepartitioningTracker[MasterReference <: Messageable]
extends ColorfulLogging {
  var master: MasterReference = _
}

object RepartitioningTracker extends Logging {
  val MASTER_ENDPOINT_NAME = "RepartitioningTrackerMaster"
  val WORKER_ENDPOINT_NAME = "RepartitioningTrackerWorker"

}
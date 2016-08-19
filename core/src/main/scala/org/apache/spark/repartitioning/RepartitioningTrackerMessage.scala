package org.apache.spark.repartitioning

import org.apache.spark.{Partitioner, ScannerPrototype}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.reflect.ClassTag

/**
  * Represents a message between repartitioning trackers.
  */
private[spark] sealed trait RepartitioningTrackerMessage
  extends Serializable

/**
  * Shuffle write status message.
  */
private[spark] case class ShuffleWriteStatus[T: ClassTag](
  stageID: Int,
  taskID: Long,
  partitionID: Int,
  keyHistogram: DataCharacteristicsAccumulator) extends RepartitioningTrackerMessage

/**
  * Registering message sent from workers.
  */
private[spark] case class Register(executorID: String, workerReferece: RpcEndpointRef)
  extends RepartitioningTrackerMessage

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class StandaloneStrategy(stageID: Int, scanner: ScannerPrototype)
  extends ScanStrategy

private[spark] class ScanStrategy extends RepartitioningTrackerMessage

private[spark] case class ScanStrategies(scanStrategies: List[ScanStrategy])
  extends RepartitioningTrackerMessage

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class ShutDownScanners(stageID: Int)
  extends RepartitioningTrackerMessage

/**
  * Scan strategy message sent to workers.
  */
private[spark] case class ClearStageData(stageID: Int)
  extends RepartitioningTrackerMessage

/**
  * Repartitioning strategy message sent to workers.
  */
private[spark] case class RepartitioningStrategy(
  stageID: Int,
  repartitioner: Partitioner,
  version: Int) extends RepartitioningTrackerMessage

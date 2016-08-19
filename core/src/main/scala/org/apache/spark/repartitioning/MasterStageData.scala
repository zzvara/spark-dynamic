package org.apache.spark.repartitioning

import org.apache.spark.Strategy
import org.apache.spark.scheduler.StageInfo

case class MasterStageData(
  info: StageInfo,
  strategy: Strategy,
  mode: RepartitioningModes.Value,
  scanStrategy: StandaloneStrategy)

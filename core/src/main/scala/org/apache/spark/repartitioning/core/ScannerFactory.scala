package org.apache.spark.repartitioning.core

import org.apache.spark.util.DataCharacteristicsAccumulator

abstract class ScannerFactory[+S <: Scanner[_, _]] extends Serializable {
  def apply(totalSlots: Int,
            histogramDrop: (Int, Long, Int, DataCharacteristicsAccumulator) => Unit): S
}

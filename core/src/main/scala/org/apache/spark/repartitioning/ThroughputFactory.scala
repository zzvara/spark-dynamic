package org.apache.spark.repartitioning

import org.apache.spark.repartitioning.core.ScannerFactory

object ThroughputFactory extends ScannerFactory[Throughput] {
  def apply(totalSlots: Int): Throughput = new Throughput(totalSlots)
}

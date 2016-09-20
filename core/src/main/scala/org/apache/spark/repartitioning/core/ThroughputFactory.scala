package org.apache.spark.repartitioning.core

import org.apache.spark.repartitioning.Throughput

object ThroughputFactory extends ScannerFactory[Throughput] {
  def apply(totalSlots: Int): Throughput = new Throughput(totalSlots)
}

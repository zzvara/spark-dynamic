package org.apache.spark.repartitioning

class ThroughputPrototype(val totalSlots: Int) extends ScannerPrototype {
  def newInstance(): Scanner = new Throughput(totalSlots)
}

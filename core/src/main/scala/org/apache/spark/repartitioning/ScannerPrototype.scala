package org.apache.spark.repartitioning

trait ScannerPrototype extends Serializable {
  def newInstance(): Scanner
}

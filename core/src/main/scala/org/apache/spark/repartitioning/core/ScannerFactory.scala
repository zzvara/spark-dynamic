package org.apache.spark.repartitioning.core

abstract class ScannerFactory[+S <: Scanner[_, _]] extends Serializable {
  def apply(totalSlots: Int): S
}

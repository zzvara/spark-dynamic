package org.apache.spark.repartitioning.core

object TaskEndReason extends Enumeration {
  val Success, Failed = Value
}

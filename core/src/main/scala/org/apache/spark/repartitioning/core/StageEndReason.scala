package org.apache.spark.repartitioning.core

object StageEndReason extends Enumeration {
  val Success, Fail = Value
}

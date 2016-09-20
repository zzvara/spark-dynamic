package org.apache.spark.repartitioning.core

abstract class Factory[E] {
  def apply[E](): E
}
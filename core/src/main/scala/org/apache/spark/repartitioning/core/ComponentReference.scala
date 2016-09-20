package org.apache.spark.repartitioning.core

abstract class ComponentReference {
  def send(message: Any): Unit
}

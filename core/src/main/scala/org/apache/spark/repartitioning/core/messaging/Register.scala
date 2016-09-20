package org.apache.spark.repartitioning.core.messaging

/**
  * Registering message sent from workers.
  */
case class Register[ComponentReference](executorID: String, workerReference: ComponentReference)
extends RepartitioningTrackerMessage

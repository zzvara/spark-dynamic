package org.apache.spark.streaming.repartitioning

import org.apache.spark.repartitioning.Decider

import scala.collection.mutable
import scala.collection.mutable.Set

/**
  *
  * @param streamID Stream is identified by the output DStream ID.
  * @param relatedJobs Mini-batches which has been spawned by this stream.
  * @param parentDStreams All the parent DStreams of the output DStream.
  */
case class MasterStreamData(
  streamID: Int,
  relatedJobs: Set[Int] = Set[Int](),
  parentDStreams: scala.collection.immutable.Set[Int]
  = scala.collection.immutable.Set[Int](),
  scanStrategy: StreamingStrategy){
  /**
    * Deciders, which are StreamingStrategies by default for each
    * inner stage. Stages are identified in a lazy manner when a task finished.
    * A task holds a corresponding DStream ID, which defines a reoccurring
    * stage in a mini-batch.
    */
  val strategies: mutable.Map[Int, Decider] =
  mutable.Map[Int, Decider]()

  def addJob(jobID: Int): MasterStreamData = {
    relatedJobs += jobID
    this
  }

  def hasParent(stream: Int): Boolean = {
    parentDStreams.contains(stream)
  }
}

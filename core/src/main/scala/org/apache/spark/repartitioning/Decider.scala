/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.repartitioning

import org.apache.spark._
import org.apache.spark.internal.ColorfulLogging
import org.apache.spark.util.DataCharacteristicsAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
  * A decider strategy, that continuously receives histograms from the physical tasks
  * and decides when and how to repartition a certain stage.
  *
  * That means a decider strategy is always bound to a specific stage in batch mode, or
  * to a stream in streaming mode.
  *
  * @todo Add support to check distance from uniform distribution.
  */
abstract class Decider(
  stageID: Int,
  resourceStateHandler: Option[() => Int] = None)
extends ColorfulLogging with Serializable {
  protected val histograms = mutable.HashMap[Int, DataCharacteristicsAccumulator]()
  protected var currentVersion: Int = 0
  /**
    * Number of repartitions happened.
    */
  protected var repartitionCount: Int = 0
  /**
    * Partitioner history.
    */
  protected val broadcastHistory = ArrayBuffer[Partitioner]()
  /**
    * Number of desired partitions to have.
    */
  protected var numberOfPartitions: Int = 1
  /**
    * Latest partitioning information, mostly to decide whether a new one is necessary.
    */
  protected var latestPartitioningInfo: Option[PartitioningInfo] = None
  /**
    * Depth of the probability-cut in the new {{KeyIsolatorPartitioner}}.
    */
  protected val treeDepthHint =
    SparkEnv.get.conf.getInt("spark.repartitioning.partitioner-tree-depth", 3)

  /**
    * Fetches the number of total slots available from an external resource-state handler.
    *
    * Should be only called on the driver when decision is to be made.
    */
  protected def totalSlots: Int = {
    resourceStateHandler
      .map(_.apply())
      .getOrElse(throw new SparkException(
        "Resource state handler is not available! Maybe called from executor?")
      )
  }

  /**
    * Any decider strategy should accept incoming histograms as {{DataCharacteristicsAccumulator}}s
    * for its partitions.
    */
  def onHistogramArrival(partitionID: Int, keyHistogram: DataCharacteristicsAccumulator): Unit

  /**
    * Validates a global histogram whether it fulfills the requirements for a sane
    * repartitioning.
    */
  protected def isValidHistogram(histogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    if (histogram.size < 2) {
      logWarning(s"Histogram size is ${histogram.size}. Invalidated.")
      false
    } else if (!histogram.forall(!_._2.isInfinity)) {
      logWarning(s"There is an infinite value in the histogram! Invalidated.")
      false
    } else {
      true
    }
  }

  protected def getPartitioningInfo(
      globalHistogram: scala.collection.Seq[(Any, Double)]): PartitioningInfo = {
    val initialInfo =
      PartitioningInfo.newInstance(globalHistogram, totalSlots, treeDepthHint)
    val multiplier = math.min(initialInfo.level / initialInfo.sortedValues.head, 2)
    numberOfPartitions = totalSlots * multiplier.ceil.toInt
    val partitioningInfo =
      PartitioningInfo.newInstance(globalHistogram, numberOfPartitions, treeDepthHint)
    logInfo(s"Constructed partitioning info is [$partitioningInfo].", "DRHistogram")
    latestPartitioningInfo = Some(partitioningInfo)
    partitioningInfo
  }


  protected def clearHistograms(): Unit

  /**
    * Decides if repartitioning is needed. If so, constructs a new
    * partitioning function and sends the strategy to each worker.
    * It asks the RepartitioningTrackerMaster to broadcast the new
    * strategy to workers.
    */
  def repartition(): Boolean = {
    val doneRepartitioning =
      if (preDecide()) {
        val globalHistogram = getGlobalHistogram
        if (decideAndValidate(globalHistogram)) {
          resetPartitioners(getNewPartitioner(getPartitioningInfo(globalHistogram)))
          true
        } else {
          logInfo("Decide-and-validate is no-go.")
          false
        }
      } else {
        logInfo("Pre-decide is no-go.")
        false
      }
    cleanup()
    doneRepartitioning
  }

  protected def preDecide(): Boolean

  /**
    * @todo Use `h.update` instead of `h.value` in batch job.
    */
  protected def getGlobalHistogram: scala.collection.Seq[(Any, Double)] = {
    val numRecords =
      histograms.values.map(_.recordsPassed).sum

    val globalHistogram =
      histograms.values.map(h => h.normalize(h.value, numRecords))
        .reduce(DataCharacteristicsAccumulator.merge[Any, Double](0.0)(
          (a: Double, b: Double) => a + b)
        )
        .toSeq.sortBy(-_._2)
    logInfo(
      globalHistogram.foldLeft(
        s"Global histogram for repartitioning " +
        s"step $repartitionCount:\n")((x, y) =>
        x + s"\t${y._1}\t->\t${y._2}\n"), "DRHistogram")
    globalHistogram
  }

  protected def decideAndValidate(globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean

  protected def getNewPartitioner(partitioningInfo: PartitioningInfo): Partitioner = {
    val sortedKeys = partitioningInfo.sortedKeys

    val repartitioner = new KeyIsolationPartitioner(
      partitioningInfo,
      WeightedHashPartitioner.newInstance(partitioningInfo, (key: Any) =>
        (MurmurHash3.stringHash((key.hashCode + 123456791).toString).toDouble
          / Int.MaxValue + 1) / 2)
    )

    logInfo("Partitioner created, simulating run with global histogram.")
    sortedKeys.foreach {
      key => logInfo(s"Key $key went to ${repartitioner.getPartition(key)}.")
    }

    logInfo(s"Decided to repartition stage $stageID.", "DRRepartitioner")
    currentVersion += 1

    repartitioner
  }

  protected def resetPartitioners(newPartitioner: Partitioner): Unit

  protected def cleanup(): Unit
}

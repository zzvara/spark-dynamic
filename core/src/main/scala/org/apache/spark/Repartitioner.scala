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

package org.apache.spark

import org.apache.spark.util.Utils

abstract class Repartitioner(val parent: Partitioner) extends Partitioner {
  def getPartition(key: Any, oldPartition: Int): Int
}

class KHeaviestPartitioner(override val parent: Partitioner,
                           val k: Int,
                           val kLargest: Array[Int],
                           val kSmallest: Array[Int])
extends Repartitioner(parent) with ColorfulLogging {
  val largestToSmallest = (0 until k).map(i => (kLargest(i), kSmallest(i))).toMap
  val smallestToLargest = largestToSmallest.map(_.swap)

  logInfo(s"Created with ($k, largest: (${kLargest.mkString(", ")})," +
    s"smallest: (${kSmallest.mkString(", ")}))", "DRRepartitioner")

  override def numPartitions: Int = parent.numPartitions

  override def getPartition(key: Any): Int = {
    val oldPartition = parent.getPartition(key)
    getPartition(key, oldPartition)
  }

  override def getPartition(key: Any, oldPartition: Int): Int = {
    if (largestToSmallest.keySet.contains(oldPartition) &&
      Utils.nonNegativeMod(key.hashCode(), numPartitions * 2) >= numPartitions) {
      largestToSmallest(oldPartition)
    } else if (smallestToLargest.keySet.contains(oldPartition) &&
      Utils.nonNegativeMod(key.hashCode(), numPartitions * 2) < numPartitions) {
      smallestToLargest(oldPartition)
    } else {
      oldPartition
    }
  }
}

class KeyIsolationPartitioner(val partitioningInfo: PartitioningInfo,
                              val heaviestKeys: Array[Any],
                              val weightedHashPartitioner: WeightedHashPartitioner)
extends Partitioner {

  assert(heaviestKeys.length == partitioningInfo.cut)
  private val heavyKeysMap = Map[Any, Int]() ++ heaviestKeys.zipWithIndex

  override def numPartitions: Int = partitioningInfo.partitions

  override def getPartition(key: Any): Int = {
    heavyKeysMap.get(key) match {
      case Some(k) => k
      case None =>
        val bucket = partitioningInfo.partitions - weightedHashPartitioner.getPartition(key) - 1
        bucket
    }
  }
}

class WeightedHashPartitioner(weights: Array[Double],
                              partitioningInfo: PartitioningInfo,
                              hash: Any => Double)
  extends Partitioner {
  private val partitions = partitioningInfo.partitions
  private val cut = partitioningInfo.cut
  private val sCut = partitioningInfo.sCut
  private val level = partitioningInfo.level
  private val block = (partitions - cut) * level
  assert(weights.length == cut - sCut)

  private val precision = numPartitions / 1.0e9
  for (i <- 0 until cut - sCut - 1 by 1) {
    assert(weights(i) >= weights(i + 1) - precision)
  }
  assert(weights.isEmpty || weights.last >= 0 - precision)
  private val aggregated = weights.scan(0.0d)(_ + _).drop(1)
  private val sum = if (cut > sCut) aggregated.last else 0

  override def numPartitions: Int = partitions - partitioningInfo.sCut

  override def getPartition(key: Any): Int = {
    val searchKey = hash(key) * (block + sum)
    val bucket = if (searchKey == 0.0d) {
      0
    } else if (searchKey <= block) {
      (searchKey / level).ceil.toInt - 1
    } else {
      BinarySearch.binarySearch(aggregated, searchKey - block) + partitions - cut
    }
    bucket
  }
}

object BinarySearch {
  def binarySearch(array: Array[Double], value: Double): Int = {
    binarySearch((i: Int) => array(i), value, 0, array.length - 1)
  }

  def binarySearch(f: Int => Double, value: Double, lower: Int, upper: Int): Int = {
    if (lower == upper) {
      lower
    } else {
      val middle = (lower + upper) / 2
      if (value <= f(middle)) {
        binarySearch(f, value, lower, middle)
      } else {
        binarySearch(f, value, middle + 1, upper)
      }
    }
  }
}
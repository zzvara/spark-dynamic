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

package org.apache.spark.examples.repartitioning

import org.apache.spark.{PartitioningInfo, WeightedHashPartitioner}

object WeightedHashPartitionerTest {

  def main(args: Array[String]): Unit = {
    test1()

    test2()

    test3()

    test4()
  }

  def test1() = {
    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)

    val p = 6
    val pCut = 4
    val level = 9.0d / 55
    val sCut = 2
    // val block = 18.0d / 55

    val weights = Array[Double](2.0d / 55, 1.0d / 55)
    val info = new PartitioningInfo(p, pCut, sCut, level, null, null)

    val partitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Double => dd
      case _ => 0.0d
    })
    assert(partitioner.numPartitions == 4)

    assert(partitioner.getPartition(0.0d) == 0)
    assert(partitioner.getPartition(8.0d / 21) == 0)
    assert(partitioner.getPartition(17.0d / 21) == 1)
    assert(partitioner.getPartition(18.0d / 21) == 1)
    assert(partitioner.getPartition(19.0d / 21) == 2)
    assert(partitioner.getPartition(20.0d / 21) == 3)//rounding error, should be 2
    assert(partitioner.getPartition(1.0d) == 3)
  }

  def test2() = {
    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)

    val p = 7
    val pCut = 3
    val level = 7.0d / 55
    val sCut = 3
    // val block = 28.0d / 55

    val weights = Array[Double]()
    val info = new PartitioningInfo(p, pCut, sCut, level, null, null)

    val partitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Double => dd
      case _ => 0.0d
    })
    assert(partitioner.numPartitions == 4)

    assert(partitioner.getPartition(0.0d) == 0)
    assert(partitioner.getPartition(6.0d / 28) == 0)
    assert(partitioner.getPartition(13.0d / 28) == 1)
    assert(partitioner.getPartition(14.0d / 28) == 1)
    assert(partitioner.getPartition(20.0d / 28) == 2)
    assert(partitioner.getPartition(27.0d / 28) == 3)
    assert(partitioner.getPartition(1.0d) == 3)
  }

  def test3() = {
    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)

    val p = 5
    val pCut = 3
    val level = 1.0d / 5
    val sCut = 0
    // val block = 2.0d / 5

    val weights = Array[Double](3.0d / 55, 2.0d / 55, 1.0d / 55)
    val info = new PartitioningInfo(p, pCut, sCut, level, null, null)

    val partitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Double => dd
      case _ => 0.0d
    })
    assert(partitioner.numPartitions == 5)

    assert(partitioner.getPartition(0.0d) == 0)
    assert(partitioner.getPartition(10.0d / 28) == 0)
    assert(partitioner.getPartition(20.0d / 28) == 1)
    assert(partitioner.getPartition(22.0d / 28) == 1)
    assert(partitioner.getPartition(24.0d / 28) == 2)
    assert(partitioner.getPartition(26.0d / 28) == 3)
    assert(partitioner.getPartition(27.0d / 28) == 4)//rounding error, should be 3
    assert(partitioner.getPartition(1.0d) == 4)
  }

  def test4() = {
    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)

    val p = 6
    val pCut = 6
    val level = 9.0d / 55
    val sCut = 2
    // val block = 0.0d

    val weights = Array[Double](4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)
    val info = new PartitioningInfo(p, pCut, sCut, level, null, null)

    val partitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Double => dd
      case _ => 0.0d
    })
    assert(partitioner.numPartitions == 4)

    assert(partitioner.getPartition(0.0d) == 0)
    assert(partitioner.getPartition(3.0d / 10) == 0)
    assert(partitioner.getPartition(6.0d / 10) == 1)
    assert(partitioner.getPartition(7.0d / 10) == 1)
    assert(partitioner.getPartition(8.0d / 10) == 2)
    assert(partitioner.getPartition(1.0d) == 3)
  }

}
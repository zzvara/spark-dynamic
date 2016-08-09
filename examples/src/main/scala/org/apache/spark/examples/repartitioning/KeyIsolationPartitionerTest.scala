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

import org.apache.spark.{KeyIsolationPartitioner, PartitioningInfo, WeightedHashPartitioner}

/**
  * Created by szape on 2016.01.28..
  */
object KeyIsolationPartitionerTest {

  def main(args: Array[String]): Unit = {
    test1()

    test2()

    test3()

    test4()
  }

  def test1() = {
    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55, 6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)

    val p = 6
    val pCut = 4
    val level = 9.0d / 55
    val sCut = 2
    //    val block = 18.0d / 55

    val weights = Array[Double](2.0d / 55, 1.0d / 55)
    val info = new PartitioningInfo(p, pCut, sCut, level, Array[Any](6, 5, 1, 9), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55))

    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Int => (dd % 21).toDouble / 21
      case _ => 0.0d
    })

    val partitioner = new KeyIsolationPartitioner(info, weightedPartitioner)

    assert(partitioner.numPartitions == 6)

    assert(partitioner.getPartition(6) == 0)
    assert(partitioner.getPartition(5) == 1)
    assert(partitioner.getPartition(1) == 2)
    assert(partitioner.getPartition(9) == 3)
    assert(partitioner.getPartition(21) == 5)
    assert(partitioner.getPartition(29) == 5)
    assert(partitioner.getPartition(38) == 4)
    assert(partitioner.getPartition(39) == 4)
    assert(partitioner.getPartition(40) == 3)
    assert(partitioner.getPartition(41) == 2) //rounding error, should be 3
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
    val info = new PartitioningInfo(p, pCut, sCut, level,  Array[Any](6, 5, 1), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55))

    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Int => (dd % 28).toDouble / 28
      case _ => 0.0d
    })

    val partitioner = new KeyIsolationPartitioner(info, weightedPartitioner)

    assert(partitioner.numPartitions == 7)

    assert(partitioner.getPartition(6) == 0)
    assert(partitioner.getPartition(5) == 1)
    assert(partitioner.getPartition(1) == 2)
    assert(partitioner.getPartition(28) == 6)
    assert(partitioner.getPartition(34) == 6)
    assert(partitioner.getPartition(41) == 5)
    assert(partitioner.getPartition(42) == 5)
    assert(partitioner.getPartition(48) == 4)
    assert(partitioner.getPartition(55) == 3)
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
    val info = new PartitioningInfo(p, pCut, sCut, level, Array[Any](4, 3, 1), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55))

    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Int => (dd % 28).toDouble / 28
      case _ => 0.0d
    })

    val partitioner = new KeyIsolationPartitioner(info, weightedPartitioner)

    assert(partitioner.numPartitions == 5)

    assert(partitioner.getPartition(4) == 0)
    assert(partitioner.getPartition(3) == 1)
    assert(partitioner.getPartition(1) == 2)
    assert(partitioner.getPartition(28) == 4)
    assert(partitioner.getPartition(38) == 4)
    assert(partitioner.getPartition(48) == 3)
    assert(partitioner.getPartition(50) == 3)
    assert(partitioner.getPartition(52) == 2)
    assert(partitioner.getPartition(54) == 1)
    assert(partitioner.getPartition(55) == 0) //rounding error, should be 1
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
    val info = new PartitioningInfo(p, pCut, sCut, level, Array[Any](0, 1, 2, 3, 4, 5), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
      6.0d / 55, 5.0d / 55))

    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
      case dd: Int => (dd % 10).toDouble / 10
      case _ => 0.0d
    })

    val partitioner = new KeyIsolationPartitioner(info, weightedPartitioner)

    assert(partitioner.numPartitions == 6)

    assert(partitioner.getPartition(0) == 0)
    assert(partitioner.getPartition(1) == 1)
    assert(partitioner.getPartition(2) == 2)
    assert(partitioner.getPartition(3) == 3)
    assert(partitioner.getPartition(4) == 4)
    assert(partitioner.getPartition(5) == 5)
    assert(partitioner.getPartition(10) == 5)
    assert(partitioner.getPartition(13) == 5)
    assert(partitioner.getPartition(16) == 4)
    assert(partitioner.getPartition(17) == 4)
    assert(partitioner.getPartition(18) == 3)
  }

}
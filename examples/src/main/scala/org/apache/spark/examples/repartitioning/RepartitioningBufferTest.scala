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

import java.util.Comparator

import org.apache.spark.{HashPartitioner, Repartitioner}
import org.apache.spark.util.collection.RepartitioningBuffer

// TODO: turn this class into a JUnit test
object RepartitioningBufferTest {

  def main(args: Array[String]): Unit = {

    val record1 = ("aa", "vaa")
    val record2 = ("ab", "vab")
    val record3 = ("ab", "vab")
    val record4 = ("ac", "vac")
    val record5 = ("ca", "vca")
    val record6 = ("ca", "vca")
    val record7 = ("cb", "vcb")
    val record8 = ("cc", "vcc")

    val record9 = ("ba", "vba")
    val record10 = ("bb", "vbb")
    val record11 = ("bc", "vbc")
    val record12 = ("bc", "vbc")

    //    val oldPartitioner = new Partitioner {
    //      override def numPartitions: Int = 2
    //
    //      override def getPartition(key: Any): Int = key.asInstanceOf[String].charAt(0).hashCode() % 2
    //    }

    val newPartitioner = new Repartitioner(new HashPartitioner(2)) {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = key.asInstanceOf[String].charAt(1).hashCode() % 2

      override def getPartition(key: Any, oldPartition: Int): Int = getPartition(key: Any)
    }

    //    println(oldPartitioner.getPartition(record1._1))
    //    println(oldPartitioner.getPartition(record6._1))
    //    println(oldPartitioner.getPartition(record11._1))
    //
    //    println(newPartitioner.getPartition(record1._1))
    //    println(newPartitioner.getPartition(record6._1))
    //    println(newPartitioner.getPartition(record11._1))

    val rpBuffer = new RepartitioningBuffer[String, String](newPartitioner)

    val partitionIterator0 = Iterator[(String, String)](record1, record2, record3, record4, record5, record6, record7, record8)
    val partitionIterator1 = Iterator[(String, String)](record9, record10, record11, record12)

    rpBuffer.insertAll(Iterator[(Int, Iterator[(String, String)])]((0, partitionIterator0), (1, partitionIterator1)))

    val keyComparator = if(args(0) == "ordering=on") {
      Some(new Comparator[String] {
        override def compare(o1: String, o2: String): Int = {
          //          println("#compared: " + o1 + ", " + o2)
          o1.compare(o2)
        }
      })
    } else {
      None
    }

    var index: Int = 0
    rpBuffer.partitionedDestructiveSortedIterator(keyComparator).foreach(x => {
      if (x._1._1 > index) {
        index = x._1._1
        println()
      }
      println(x + ", ")
    })
  }

}

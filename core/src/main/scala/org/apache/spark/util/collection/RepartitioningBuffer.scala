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

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.Partitioner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RepartitioningBuffer[K, V](val newPartitioner: Partitioner)
  extends WritablePartitionedPairCollection[K, V] {

  val numPartitions = newPartitioner.numPartitions
  val buffers =
    Array.fill[mutable.Map[Int, ArrayBuffer[(K, V)]]](numPartitions)(
      mutable.Map[Int, ArrayBuffer[(K, V)]]())

  def insertAll(iterators: Iterator[(Int, Iterator[Product2[K, V]])]): Unit = {
    iterators.foreach(iter => iter._2.foreach(x => insert(x._1, x._2, iter._1)))
  }

  /**
    * Insert a key-value pair with a partition into the collection
    */
  override def insert(newPartition: Int, key: K, value: V): Unit = {
    throw new RuntimeException("This kind of insertion is not supported by RepartitioningBuffer. " +
      "Use method RepartitioningBuffer.insert(key: K, value: V, oldPartition: Int) instead.")
    //    val oldPartition = oldPartitioner.getPartition(key)
    //    val map = buffers(newPartition)
    //    map.get(oldPartition) match {
    //      case None => map.update(oldPartition, ArrayBuffer[(K, V)]((key, value)))
    //      case Some(buff) => buff.append((key, value))
    //    }
  }

  /**
    * Insert a key-value pair with a partition into the collection
    */
  def insert(key: K, value: V, oldPartition: Int): Unit = {
    val newPartition = newPartitioner.getPartition(key)
    val map = buffers(newPartition)
    map.get(oldPartition) match {
      case None => map.update(oldPartition, ArrayBuffer[(K, V)]((key, value)))
      case Some(buff) => buff.append((key, value))
    }
  }

  /**
    * Iterate through the data in order of partition ID and then the given comparator. This may
    * destroy the underlying collection.
    */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]]):
  Iterator[((Int, K), V)] = {
    try {
      (0 until numPartitions)
        .map(p => (
          keyComparator match {
            case Some(kc) => mergeSort(buffers(p).toSeq.map(_._2.iterator), kc)
            case None => buffers(p).values.map(_.iterator).foldLeft(Iterator[(K, V)]())(_ ++ _)
          }).map(m => ((p, m._1), m._2)))
        .reduce(_ ++ _)
    } catch {
      case ex: Exception => throw new RuntimeException("Repartitioning of records failed")
    }
  }

  /**
    * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
    */
  private def mergeSort(iterators: Seq[Iterator[(K, V)]], comparator: Comparator[K])
  : Iterator[(K, V)] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[(K, V)]
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    heap.enqueue(bufferedIters: _*) // Will contain only the iterators with hasNext = true
    new Iterator[(K, V)] {
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }
}
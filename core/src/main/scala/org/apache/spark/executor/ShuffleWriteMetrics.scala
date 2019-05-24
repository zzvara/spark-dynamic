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

package org.apache.spark.executor

import org.apache.spark.Partitioner
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.{DataCharacteristicsAccumulator, LongAccumulator}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  private[executor] val _repartitioningTime = new LongAccumulator
  private[executor] val _insertionTime = new LongAccumulator
  private[executor] val _dataCharacteristics = new DataCharacteristicsAccumulator

  private var _repartitioner: Option[Partitioner] = None
  private var _repartitioningIsFinished = false

  // TODO: Getter should not make it a None
  def repartitioner: Option[Partitioner] = {
    val result = _repartitioner
    _repartitioner = None
    result
  }

  private[spark] def setDataCharacteristics(s: Map[Any, Double]): Unit = {
    _dataCharacteristics.setValue(s)
  }

  private[spark] def setRepartitioner(repartitioner: Partitioner) {
    _repartitioner = Some(repartitioner)
  }


  // TODO: Why is there a flag?
  private[spark] def finishRepartitioning(flag: Boolean = true) {
    if (flag) {
      _repartitioningIsFinished = true
    }
  }

  def dataCharacteristics: DataCharacteristicsAccumulator = _dataCharacteristics


  // TODO: Compact data characteristics.
  def compact(): Unit = {}

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum

  /**
    * Time the task spent repartitioning the previously written data.
    */
  def repartitioningTime: Long = _repartitioningTime.sum

  def insertionTime: Long = _insertionTime.sum

  private[spark] def addKeyWritten(k: Any, complexity: Int): Unit = {
    _dataCharacteristics.add((k, complexity))
  }

  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] override def incRepartitioningTime(v: Long): Unit = _repartitioningTime.add(v)
  private[spark] override def incInsertionTime(v: Long): Unit = _insertionTime.add(v)
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
}

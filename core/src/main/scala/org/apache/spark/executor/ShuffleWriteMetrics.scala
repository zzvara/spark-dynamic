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
import org.apache.spark.internal.ColorfulLogging
import org.apache.spark.util.{DataCharacteristicsAccumulator, LongAccumulator}

import scala.collection.mutable


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  private[executor] val _repartitioningTime = new LongAccumulator
  private[executor] val _insertionTime = new LongAccumulator
  private[executor] val _dataCharacteristics = new DataCharacteristicsAccumulator

  private var _repartitioner: Option[Partitioner] = None
  private var _repartitioningIsFinished = false

  /**
    * @todo Getter should not make it a None.
    */
  def repartitioner: Option[Partitioner] = {
    val result = _repartitioner
    _repartitioner = None
    result
  }

  private[spark] def setDataCharacteristics(s: Map[Any, Double]): Unit = {
    _dataCharacteristics.setValue(mutable.Map[Any, Double]() ++ s)
  }

  private[spark] def setRepartitioner(repartitioner: Partitioner) {
    /*
    logDebug(s"Received repartitioner for stage ${stageID.getOrElse("?")}" +
      s" task ${taskID.getOrElse("?")}", "DRRepartitioning")
      */
    _repartitioner = Some(repartitioner)
  }

  /**
    * @todo Why is there a flag?
    */
  private[spark] def finishRepartitioning(flag: Boolean = true) {
    if (flag) {
      _repartitioningIsFinished = true
      // logInfo(s"Repartitioning finished for stage ${stageID.get}.", "DRRepartitioning")
    }
  }

  def dataCharacteristics: DataCharacteristicsAccumulator = _dataCharacteristics

  /**
    * @todo Compact data characteristics.
    */
  def compact(): Unit = {

  }

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
    _dataCharacteristics.add((k, complexity) -> 1.0)
  }

  private[spark] def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] def incRepartitioningTime(v: Long): Unit = _repartitioningTime.add(v)
  private[spark] def incInsertionTime(v: Long): Unit = _insertionTime.add(v)
  private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }

  // Legacy methods for backward compatibility.
  // TODO: remove these once we make this class private.
  @deprecated("use bytesWritten instead", "2.0.0")
  def shuffleBytesWritten: Long = bytesWritten
  @deprecated("use writeTime instead", "2.0.0")
  def shuffleWriteTime: Long = writeTime
  @deprecated("use recordsWritten instead", "2.0.0")
  def shuffleRecordsWritten: Long = recordsWritten
}

class RepartitioningInfo(
  val stageID: Int,
  val taskID: Long,
  val executorName: String,
  val taskMetrics: TaskMetrics,
  var repartitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0)) extends Serializable with ColorfulLogging {

  var trackingFinished = false

  logInfo(s"Created RepartitioningInfo for stage:$stageID-task:$taskID.\n\tRepartitioner: $repartitioner, version: $version.", "yellow")

  def updateRepartitioner(repartitioner: Partitioner, version: Int): Unit = {
    this.repartitioner = Some(repartitioner)
    this.version = Some(version)
    logInfo(s"Updated repartitioner for stage:$stageID-task:$taskID.\n\tNew repartitioner: $repartitioner, new version: $version.", "yellow")
  }

  def getHistogramMeta: DataCharacteristicsAccumulator = {
    taskMetrics.shuffleWriteMetrics.dataCharacteristics
  }

  def finishTracking(): Unit = {
    trackingFinished = true
    logInfo(s"Finished tracking of stage: $stageID, task:$taskID." +
            s"\n\tRepartitioner: $repartitioner, version: $version.", "yellow")
  }
}
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

import org.apache.spark.{Accumulator, InternalAccumulator, Partitioner}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator
import org.apache.spark.AccumulatorParam.DataCharacteristicsAccumulatorParam
import org.apache.spark.executor.ShuffleWriteMetrics.DataCharacteristics
import org.apache.spark.{ColorfulLogging, Partitioner, Accumulator, InternalAccumulator}


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
  private[executor] val _dataCharacteristics = new Accumulator[Map[Any, Double]](???)

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

  def dataCharacteristics: DataCharacteristics[Any] = _dataCharacteristics

  def compact(): Unit = {
    _dataCharacteristics.compact()
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

  private[spark] def addKeyWritten(k: Any): Unit = {
    _dataCharacteristics.add(Map[Any, Double](k -> 1.0))
  }
  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
}

object ShuffleWriteMetrics {
  type DataCharacteristics[T] = Accumulator[Map[T, Double]]
}

class RepartitioningInfo(
  val stageID: Int,
  val taskID: Long,
  val executorName: String,
  val taskMetrics: TaskMetrics,
  var repartitioner: Option[Partitioner] = None,
  var version: Option[Int] = Some(0)) extends Serializable with ColorfulLogging {

  var trackingFinished = false

  def updateRepartitioner(repartitioner: Partitioner, version: Int): Unit = {
    this.repartitioner = Some(repartitioner)
    this.version = Some(version)
  }

  def getHistogramMeta: Option[DataCharacteristicsAccumulatorParam] = {
    taskMetrics.shuffleWriteMetrics.map {
      _.dataCharacteristics.getParam.asInstanceOf[DataCharacteristicsAccumulatorParam]
    }
  }

  def finishTracking(): Unit = {
    trackingFinished = true
  }
}
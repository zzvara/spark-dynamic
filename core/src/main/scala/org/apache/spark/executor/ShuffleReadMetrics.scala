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

import java.util.ArrayList

import org.apache.spark.AccumulatorParam.ListAccumulatorParam
import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.BlockFetchInfo
import org.apache.spark.storage.BlockResult
import org.apache.spark.util.{CollectionAccumulator, DataCharacteristicsAccumulator, LongAccumulator}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable with Logging {
  private[executor] val _remoteBlocksFetched = new LongAccumulator
  private[executor] val _remoteBlockFetchInfos = new CollectionAccumulator[BlockFetchInfo]
  private[executor] val _localBlocksFetched = new LongAccumulator
  private[executor] val _localBlockFetchInfos = new CollectionAccumulator[BlockFetchInfo]
  private[executor] val _remoteBytesRead = new LongAccumulator
  private[executor] val _localBytesRead = new LongAccumulator
  private[executor] val _fetchWaitTime = new LongAccumulator
  private[executor] val _recordsRead = new LongAccumulator
  private[executor] val _dataCharacteristics = new DataCharacteristicsAccumulator

  val recordCharacteristics: Boolean =
    SparkEnv.get.conf.getBoolean("spark.metrics.shuffleRead.dataCharacteristics", true)

  def dataCharacteristics(): DataCharacteristicsAccumulator = _dataCharacteristics

  private[spark] def recordIterator(iter: Iterator[(_, _)]): Iterator[(_, _)] = {
    if (recordCharacteristics) {
      iter.map {
        pair =>
          _dataCharacteristics.add(pair._1 -> 1.0)
          pair
      }
    } else {
      iter
    }
  }

  /**
    * @todo Fix.
    */
  def compact(): Unit = {

  }

  private[spark] def addBlockFetch(blockResult: BlockResult) : Unit = {
    logDebug(s"Recording shuffle block result ${blockResult.blockId}.")
    blockResult.loc match {
      case Some(loc) =>
        _remoteBlockFetchInfos.add(
          new BlockFetchInfo(blockResult.blockId,
            blockResult.bytes,
            Some(loc.executorId),
            Some(loc.host)))
      case _ =>
        _localBlockFetchInfos.add(new BlockFetchInfo(blockResult.blockId,
            blockResult.bytes))
    }
  }

  private[spark] def addBlockFetch(blockFetchInfo: BlockFetchInfo) : Unit = {
    logDebug(s"Recording shuffle block fetch ${blockFetchInfo.blockId}.")
    blockFetchInfo.host match {
      case Some(host) => _remoteBlockFetchInfos.add(blockFetchInfo)
      case _ => _localBlockFetchInfos.add(blockFetchInfo)
    }
  }

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.sum

  /**
    * @todo What?
    */
  def remoteBlockFetchInfos(): Seq[BlockFetchInfo] = _remoteBlockFetchInfos.value.asScala.toSeq

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Long = _localBlocksFetched.sum

  /**
    * @todo What?
    */
  def localBlockFetchInfos(): Seq[BlockFetchInfo] = _localBlockFetchInfos.value.asScala.toSeq

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.sum

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.sum

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.sum

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)

  private[spark] def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
  private[spark] def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
  private[spark] def setRemoteBlockFetchInfos(s: java.util.List[BlockFetchInfo]): Unit = {
    s.asScala.foreach {
      _remoteBlockFetchInfos.add
    }
  }
  private[spark] def setLocalBlockFetchInfos(s: java.util.List[BlockFetchInfo]): Unit = {
    s.asScala.foreach {
      _localBlockFetchInfos.add
    }
  }

  private[spark] def setDataCharacteristics(s: Map[Any, Double]): Unit = {
    _dataCharacteristics.setValue(mutable.Map[Any, Double]() ++ s)
  }

  /**
   * Resets the value of the current metrics (`this`) and and merges all the independent
   * [[TempShuffleReadMetrics]] into `this`.
   * @todo Add support for block fetch infos.
   */
  private[spark] def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit = {
    _remoteBlocksFetched.setValue(0)
    _localBlocksFetched.setValue(0)
    _remoteBytesRead.setValue(0)
    _localBytesRead.setValue(0)
    _fetchWaitTime.setValue(0)
    _recordsRead.setValue(0)
    metrics.foreach { metric =>
      _remoteBlocksFetched.add(metric.remoteBlocksFetched)
      _localBlocksFetched.add(metric.localBlocksFetched)
      _remoteBytesRead.add(metric.remoteBytesRead)
      _localBytesRead.add(metric.localBytesRead)
      _fetchWaitTime.add(metric.fetchWaitTime)
      _recordsRead.add(metric.recordsRead)
    }
  }
}

/**
 * A temporary shuffle read metrics holder that is used to collect shuffle read metrics for each
 * shuffle dependency, and all temporary metrics will be merged into the [[ShuffleReadMetrics]] at
 * last.
 */
private[spark] class TempShuffleReadMetrics extends Logging {
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _remoteBlockFetchInfos = new ArrayList[BlockFetchInfo]()
  private[this] var _localBlocksFetched = 0L
  private[this] var _localBlockFetchInfos = new ArrayList[BlockFetchInfo]()
  private[this] var _remoteBytesRead = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L
  private[this] var _dataCharacteristics = new DataCharacteristicsAccumulator


  def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
  def addRemoteBlockFetchInfo(v: BlockFetchInfo): Unit = _remoteBlockFetchInfos.add(v)
  def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched += v
  def addLocalBlockFetchInfo(v: BlockFetchInfo): Unit = _localBlockFetchInfos.add(v)
  def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  def incRecordsRead(v: Long): Unit = _recordsRead += v

  private[spark] def addBlockFetch(blockFetchInfo: BlockFetchInfo) : Unit = {
    logDebug(s"Recording shuffle block fetch ${blockFetchInfo.blockId}.")
    blockFetchInfo.host match {
      case Some(host) => _remoteBlockFetchInfos.add(blockFetchInfo)
      case _ => _localBlockFetchInfos.add(blockFetchInfo)
    }
  }

  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def remoteBlockFetchInfos: java.util.List[BlockFetchInfo] = _remoteBlockFetchInfos
  def localBlocksFetched: Long = _localBlocksFetched
  def localBlockFetchInfos: java.util.List[BlockFetchInfo] = _localBlockFetchInfos
  def remoteBytesRead: Long = _remoteBytesRead
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead

  val recordCharacteristics: Boolean =
    SparkEnv.get.conf.getBoolean("spark.metrics.shuffleRead.dataCharacteristics", true)

  private[spark] def recordIterator(iter: Iterator[(_, _)]): Iterator[(_, _)] = {
    if (recordCharacteristics) {
      iter.map {
        pair =>
          _dataCharacteristics.add(pair._1 -> 1.0)
          pair
      }
    } else {
      iter
    }
  }

}

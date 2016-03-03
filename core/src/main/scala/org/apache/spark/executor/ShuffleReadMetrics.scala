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

import org.apache.spark.status.api.v1.BlockFetchInfo
import org.apache.spark.storage.BlockResult
import org.apache.spark.{Logging, Accumulator, InternalAccumulator}
import org.apache.spark.annotation.DeveloperApi
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleReadMetrics private (
    _remoteBlocksFetched: Accumulator[Int],
    _remoteBlockFetchInfos: Accumulator[Seq[BlockFetchInfo]],
    _localBlocksFetched: Accumulator[Int],
    _localBlockFetchInfos: Accumulator[Seq[BlockFetchInfo]],
    _remoteBytesRead: Accumulator[Long],
    _localBytesRead: Accumulator[Long],
    _fetchWaitTime: Accumulator[Long],
    _recordsRead: Accumulator[Long])
  extends Serializable with Logging {

  private[executor] def this(accumMap: Map[String, Accumulator[_]]) {
    this(
      TaskMetrics.getAccumulator[Int]
        (accumMap, InternalAccumulator.shuffleRead.REMOTE_BLOCKS_FETCHED),
      TaskMetrics.getAccumulator[Seq[BlockFetchInfo]]
        (accumMap, InternalAccumulator.shuffleRead.REMOTE_BLOCK_FETCH_INFOS),
      TaskMetrics.getAccumulator[Int]
        (accumMap, InternalAccumulator.shuffleRead.LOCAL_BLOCKS_FETCHED),
      TaskMetrics.getAccumulator[Seq[BlockFetchInfo]]
        (accumMap, InternalAccumulator.shuffleRead.LOCAL_BLOCK_FETCH_INFOS),
      TaskMetrics.getAccumulator[Long](accumMap, InternalAccumulator.shuffleRead.REMOTE_BYTES_READ),
      TaskMetrics.getAccumulator[Long](accumMap, InternalAccumulator.shuffleRead.LOCAL_BYTES_READ),
      TaskMetrics.getAccumulator[Long](accumMap, InternalAccumulator.shuffleRead.FETCH_WAIT_TIME),
      TaskMetrics.getAccumulator[Long](accumMap, InternalAccumulator.shuffleRead.RECORDS_READ))
  }

  /**
   * Create a new [[ShuffleReadMetrics]] that is not associated with any particular task.
   *
   * This mainly exists for legacy reasons, because we use dummy [[ShuffleReadMetrics]] in
   * many places only to merge their values together later. In the future, we should revisit
   * whether this is needed.
   *
   * A better alternative is [[TaskMetrics.registerTempShuffleReadMetrics]] followed by
   * [[TaskMetrics.mergeShuffleReadMetrics]].
   */
  private[spark] def this() {
    this(InternalAccumulator.createShuffleReadAccumulables().map { a => (a.name.get, a) }.toMap)
  }

  private[spark] def addBlockFetch(blockResult: BlockResult) : Unit = {
    logInfo(s"Recording block result ${blockResult.blockId}.")
    blockResult.loc match {
      case Some(loc) =>
        _remoteBlockFetchInfos.add(
          mutable.Seq(new BlockFetchInfo(blockResult.blockId,
                                         blockResult.bytes,
                                         Some(loc.executorId),
                                         Some(loc.host))))
      case _ =>
        _localBlockFetchInfos.add(
          mutable.Seq(new BlockFetchInfo(blockResult.blockId,
                                         blockResult.bytes)))
    }
  }

  private[spark] def addBlockFetch(blockFetchInfo: BlockFetchInfo) : Unit = {
    logInfo(s"Recording block fetch ${blockFetchInfo.blockId}.")
    blockFetchInfo.host match {
      case Some(host) => _remoteBlockFetchInfos.add(mutable.Seq(blockFetchInfo))
      case _ => _localBlockFetchInfos.add(mutable.Seq(blockFetchInfo))
    }
  }

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Int = _remoteBlocksFetched.localValue

  /**
    * @todo What?
    */
  def remoteBlockFetchInfos(): Seq[BlockFetchInfo] = _remoteBlockFetchInfos.localValue

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Int = _localBlocksFetched.localValue

  /**
    * @todo What?
    */
  def localBlockFetchInfos(): Seq[BlockFetchInfo] = _localBlockFetchInfos.localValue

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.localValue

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.localValue

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.localValue

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.localValue

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Int = remoteBlocksFetched + localBlocksFetched

  private[spark] def incRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.add(v)
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
  private[spark] def setRemoteBlockFetchInfos(s: ArrayBuffer[BlockFetchInfo]): Unit =
    _remoteBlockFetchInfos.merge(s)
  private[spark] def setLocalBlockFetchInfos(s: ArrayBuffer[BlockFetchInfo]): Unit =
    _localBlockFetchInfos.merge(s)

}

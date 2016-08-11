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

/**
 * A collection of fields and methods concerned with internal accumulators that represent
 * task level metrics.
 */
private[spark] object InternalAccumulator {
  // Prefixes used in names of internal task level metrics
  val METRICS_PREFIX = "internal.metrics."
  val SHUFFLE_READ_METRICS_PREFIX = METRICS_PREFIX + "shuffle.read."
  val SHUFFLE_WRITE_METRICS_PREFIX = METRICS_PREFIX + "shuffle.write."
  val OUTPUT_METRICS_PREFIX = METRICS_PREFIX + "output."
  val INPUT_METRICS_PREFIX = METRICS_PREFIX + "input."

  // Names of internal task level metrics
  val EXECUTOR_DESERIALIZE_TIME = METRICS_PREFIX + "executorDeserializeTime"
  val EXECUTOR_RUN_TIME = METRICS_PREFIX + "executorRunTime"
  val RESULT_SIZE = METRICS_PREFIX + "resultSize"
  val JVM_GC_TIME = METRICS_PREFIX + "jvmGCTime"
  val RESULT_SERIALIZATION_TIME = METRICS_PREFIX + "resultSerializationTime"
  val MEMORY_BYTES_SPILLED = METRICS_PREFIX + "memoryBytesSpilled"
  val DISK_BYTES_SPILLED = METRICS_PREFIX + "diskBytesSpilled"
  val PEAK_EXECUTION_MEMORY = METRICS_PREFIX + "peakExecutionMemory"
  val UPDATED_BLOCK_STATUSES = METRICS_PREFIX + "updatedBlockStatuses"
  val BLOCK_FETCH_INFOS = METRICS_PREFIX + "blockFetchInfos"
  val TEST_ACCUM = METRICS_PREFIX + "testAccumulator"

  // scalastyle:off

  // Names of shuffle read metrics
  object shuffleRead {
    val REMOTE_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "remoteBlocksFetched"
    val REMOTE_BLOCK_FETCH_INFOS = SHUFFLE_READ_METRICS_PREFIX + "remoteBlockFetchInfos"
    val LOCAL_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "localBlocksFetched"
    val LOCAL_BLOCK_FETCH_INFOS = SHUFFLE_READ_METRICS_PREFIX + "localBlockFetchInfos"
    val REMOTE_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "remoteBytesRead"
    val LOCAL_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "localBytesRead"
    val FETCH_WAIT_TIME = SHUFFLE_READ_METRICS_PREFIX + "fetchWaitTime"
    val RECORDS_READ = SHUFFLE_READ_METRICS_PREFIX + "recordsRead"
    val DATA_CHARACTERISTICS = SHUFFLE_READ_METRICS_PREFIX + "dataCharacteristics"
  }

  // Names of shuffle write metrics
  object shuffleWrite {
    val BYTES_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "bytesWritten"
    val RECORDS_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "recordsWritten"
    val WRITE_TIME = SHUFFLE_WRITE_METRICS_PREFIX + "writeTime"
    val REPARTITIONING_TIME = SHUFFLE_WRITE_METRICS_PREFIX + "repartitioningTime"
    val INSERTION_TIME = SHUFFLE_WRITE_METRICS_PREFIX + "insertionTime"
    val DATA_CHARACTERISTICS = SHUFFLE_WRITE_METRICS_PREFIX + "dataCharacteristics"
  }

  // Names of output metrics
  object output {
    val BYTES_WRITTEN = OUTPUT_METRICS_PREFIX + "bytesWritten"
    val RECORDS_WRITTEN = OUTPUT_METRICS_PREFIX + "recordsWritten"
  }

  // Names of input metrics
  object input {
    val BYTES_READ = INPUT_METRICS_PREFIX + "bytesRead"
    val RECORDS_READ = INPUT_METRICS_PREFIX + "recordsRead"
  }

  // scalastyle:on
}

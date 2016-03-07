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

import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import org.apache.spark.status.api.v1.BlockFetchInfo

import scala.collection.mutable
import scala.ref.WeakReference

import org.apache.spark.storage.{BlockId, BlockStatus}


/**
 * A simpler value of [[Accumulable]] where the result type being accumulated is the same
 * as the types of elements being merged, i.e. variables that are only "added" to through an
 * associative and commutative operation and can therefore be efficiently supported in parallel.
 * They can be used to implement counters (as in MapReduce) or sums. Spark natively supports
 * accumulators of numeric value types, and programmers can add support for new types.
 *
 * An accumulator is created from an initial value `v` by calling [[SparkContext#accumulator]].
 * Tasks running on the cluster can then add to it using the [[Accumulable#+=]] operator.
 * However, they cannot read its value. Only the driver program can read the accumulator's value,
 * using its value method.
 *
 * The interpreter session below shows an accumulator being used to add up the elements of an array:
 *
 * {{{
 * scala> val accum = sc.accumulator(0)
 * accum: spark.Accumulator[Int] = 0
 *
 * scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
 * ...
 * 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
 *
 * scala> accum.value
 * res2: Int = 10
 * }}}
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `T`
 * @param name human-readable name associated with this accumulator
 * @param internal whether this accumulator is used internally within Spark only
 * @param countFailedValues whether to accumulate values from failed tasks
 * @tparam T result type
 */
class Accumulator[T] private[spark] (
    // SI-8813: This must explicitly be a private val, or else scala 2.11 doesn't compile
    @transient private val initialValue: T,
    param: AccumulatorParam[T],
    name: Option[String],
    internal: Boolean,
    private[spark] override val countFailedValues: Boolean = false)
  extends Accumulable[T, T](initialValue, param, name, internal, countFailedValues) {

  def this(initialValue: T, param: AccumulatorParam[T], name: Option[String]) = {
    this(initialValue, param, name, false /* internal */)
  }

  def this(initialValue: T, param: AccumulatorParam[T]) = {
    this(initialValue, param, None, false /* internal */)
  }
}


// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private[spark] object Accumulators extends Logging {
  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  @GuardedBy("Accumulators")
  val originals = mutable.Map[Long, WeakReference[Accumulable[_, _]]]()

  private val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[Accumulable]].
   * Note: Once you copy the [[Accumulable]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an [[Accumulable]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[Accumulable]] with the same ID was already registered, this does nothing instead
   * of overwriting it. This happens when we copy accumulators, e.g. when we reconstruct
   * [[org.apache.spark.executor.TaskMetrics]] from accumulator updates.
   */
  def register(a: Accumulable[_, _]): Unit = synchronized {
    if (!originals.contains(a.id)) {
      originals(a.id) = new WeakReference[Accumulable[_, _]](a)
    }
  }

  /**
   * Unregister the [[Accumulable]] with the given ID, if any.
   */
  def remove(accId: Long): Unit = synchronized {
    originals.remove(accId)
  }

  /**
   * Return the [[Accumulable]] registered with the given ID, if any.
   */
  def get(id: Long): Option[Accumulable[_, _]] = synchronized {
    originals.get(id).map { weakRef =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      weakRef.get.getOrElse {
        throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
    }
  }

  /**
   * Clear all registered [[Accumulable]]s. For testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }

}


/**
 * A simpler version of [[org.apache.spark.AccumulableParam]] where the only data type you can add
 * in is the same type as the accumulated value. An implicit AccumulatorParam object needs to be
 * available when you create Accumulators of a specific type.
 *
 * @tparam T type of value to accumulate
 */
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T): T = {
    addInPlace(t1, t2)
  }
}


object AccumulatorParam {

  // The following implicit objects were in SparkContext before 1.2 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, as there are duplicate codes in SparkContext for backward
  // compatibility, please update them accordingly if you modify the following implicit objects.

  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double): Double = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int): Int = 0
  }

  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long): Long = t1 + t2
    def zero(initialValue: Long): Long = 0L
  }

  implicit object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float): Float = t1 + t2
    def zero(initialValue: Float): Float = 0f
  }

  // Note: when merging values, this param just adopts the newer value. This is used only
  // internally for things that shouldn't really be accumulated across tasks, like input
  // read method, which should be the same across all tasks in the same stage.
  private[spark] object StringAccumulatorParam extends AccumulatorParam[String] {
    def addInPlace(t1: String, t2: String): String = t2
    def zero(initialValue: String): String = ""
  }

  // Note: this is expensive as it makes a copy of the list every time the caller adds an item.
  // A better way to use this is to first accumulate the values yourself then them all at once.
  private[spark] class ListAccumulatorParam[T] extends AccumulatorParam[Seq[T]] {
    def addInPlace(t1: Seq[T], t2: Seq[T]): Seq[T] = t1 ++ t2
    def zero(initialValue: Seq[T]): Seq[T] = Seq.empty[T]
  }

  private[spark] class MapAccumulatorParam[K, V] extends AccumulatorParam[Map[K, V]] {
    def addInPlace(t1: Map[K, V], t2: Map[K, V]): Map[K, V] = t1 ++ t2
    def zero(initialValue: Map[K, V]): Map[K, V] = Map.empty[K, V]
  }

  // For the internal metric that records what blocks are updated in a particular task
  private[spark] object UpdatedBlockStatusesAccumulatorParam
    extends ListAccumulatorParam[(BlockId, BlockStatus)]

  private[spark] object LocalBlockFetchInfosAccumulatorParam
    extends ListAccumulatorParam[BlockFetchInfo]

  private[spark] object RemoteBlockFetchInfosAccumulatorParam
    extends ListAccumulatorParam[BlockFetchInfo]

  private[spark] object ShuffleWriteDataCharacteristicsAccumulatorParam
    extends DataCharacteristicsAccumulatorParam

  private[spark] object ShuffleReadDataCharacteristicsAccumulatorParam
    extends DataCharacteristicsAccumulatorParam

  private[spark] class DataCharacteristicsAccumulatorParam
    extends MapAccumulatorParam[Any, Double] {
    @transient private val TAKE: Int = 4
    @transient private val HISTOGRAM_SCALE_BOUNDARY: Double = 20
    @transient private val BACKOFF_FACTOR: Double = 2.0
    @transient private val DROP_BOUNDARY: Double = 0.01
    @transient private val HISTOGRAM_SIZE_BOUNDARY: Int = 100
    @transient private val HISTOGRAM_COMPACTION: Int = 60
    /**
      * Rate in which records are put into the histogram.
      * Value represent that each n-th input is recorded.
      */
    private var sampleRate: Double = 1.0
    /**
      * Size or width of the histogram, that is equal to the size of the map.
      */
    private var histogramSize: Int = 0

    override def addAccumulator(r: Map[Any, Double], t: Map[Any, Double]): Map[Any, Double] = {
      val key = t.toList.head._1
      if (Math.random() <= sampleRate) { // Decided to record the key.
        val updatedHistogram = r + ((key,
          r.get(key) match {
            case Some(value) => value + 1.0
            case None =>
              histogramSize = histogramSize + 1
              sampleRate
          }
        ))
        // Decide if scaling is needed.
        if (histogramSize * sampleRate >= HISTOGRAM_SCALE_BOUNDARY) {
          sampleRate = sampleRate / BACKOFF_FACTOR
          val scaledHistogram =
            updatedHistogram
              .mapValues(x => x / BACKOFF_FACTOR)
              .filter(pair => pair._2 > DROP_BOUNDARY)
          // Decide if additional cut is needed.y
          if (histogramSize > HISTOGRAM_SIZE_BOUNDARY) {
            histogramSize = HISTOGRAM_COMPACTION
            scaledHistogram.toSeq.sortBy(-_._2).take(HISTOGRAM_COMPACTION).toMap
          } else {
            scaledHistogram
          }
        } else { // No need to cut the histogram.
          updatedHistogram
        }
      } else { // Histogram does not change.
        r
      }
    }

    override def addInPlace(t1: Map[Any, Double], t2: Map[Any, Double]): Map[Any, Double] = {
      merge[Any, Double](t1, t2)(_ + _)(0.0)
    }

    private def merge[A, B](s1: Map[A, B], s2: Map[A, B])(f: (B, B) => B)(zero: B): Map[A, B] = {
      s1 ++ s2.map{ case (k, v) => k -> f(v, s1.getOrElse(k, zero)) }
    }

    override def compact(t: Map[Any, Double]): Map[Any, Double] = {
      t.toSeq.sortBy(-_._2).take(TAKE).toMap
    }
  }
}

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

package org.apache.spark.util

import java.{lang => jl}
import java.io.ObjectInputStream
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.{InternalAccumulator, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.scheduler.AccumulableInfo

import scala.collection.mutable
import scala.reflect.ClassTag


private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean) extends Serializable


/**
 * The base class for accumulators, that can accumulate inputs of type `IN`, and produce output of
 * type `OUT`.
 */
abstract class AccumulatorV2[IN, OUT] extends Serializable {
  private[spark] var metadata: AccumulatorMetadata = _
  private[this] var atDriverSide = true

  private[spark] def register(
      sc: SparkContext,
      name: Option[String] = None,
      countFailedValues: Boolean = false): Unit = {
    if (this.metadata != null) {
      throw new IllegalStateException("Cannot register an Accumulator twice.")
    }
    this.metadata = AccumulatorMetadata(AccumulatorContext.newId(), name, countFailedValues)
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  /**
   * Returns true if this accumulator has been registered.  Note that all accumulators must be
   * registered before use, or it will throw exception.
   */
  final def isRegistered: Boolean =
    metadata != null && AccumulatorContext.get(metadata.id).isDefined

  private def assertMetadataNotNull(): Unit = {
    if (metadata == null) {
      throw new IllegalAccessError("The metadata of this accumulator has not been assigned yet.")
    }
  }

  /**
   * Returns the id of this accumulator, can only be called after registration.
   */
  final def id: Long = {
    assertMetadataNotNull()
    metadata.id
  }

  /**
   * Returns the name of this accumulator, can only be called after registration.
   */
  final def name: Option[String] = {
    assertMetadataNotNull()
    metadata.name
  }

  /**
   * Whether to accumulate values from failed tasks. This is set to true for system and time
   * metrics like serialization time or bytes spilled, and false for things with absolute values
   * like number of input rows.  This should be used for internal metrics only.
   */
  private[spark] final def countFailedValues: Boolean = {
    assertMetadataNotNull()
    metadata.countFailedValues
  }

  /**
   * Creates an [[AccumulableInfo]] representation of this [[AccumulatorV2]] with the provided
   * values.
   */
  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  final private[spark] def isAtDriverSide: Boolean = atDriverSide

  /**
   * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
   * value; for a list accumulator, Nil is zero value.
   */
  def isZero: Boolean

  /**
   * Creates a new copy of this accumulator, which is zero value. i.e. call `isZero` on the copy
   * must return true.
   */
  def copyAndReset(): AccumulatorV2[IN, OUT] = {
    val copyAcc = copy()
    copyAcc.reset()
    copyAcc
  }

  /**
   * Creates a new copy of this accumulator.
   */
  def copy(): AccumulatorV2[IN, OUT]

  /**
   * Resets this accumulator, which is zero value. i.e. call `isZero` must
   * return true.
   */
  def reset(): Unit

  /**
   * Takes the inputs and accumulates.
   */
  def add(v: IN): Unit

  /**
   * Merges another same-type accumulator into this one and update its state, i.e. this should be
   * merge-in-place.
   */
  def merge(other: AccumulatorV2[IN, OUT]): Unit

  /**
   * Defines the current value of this accumulator
   */
  def value: OUT

  // Called by Java when serializing an object
  final protected def writeReplace(): Any = {
    if (atDriverSide) {
      if (!isRegistered) {
        throw new UnsupportedOperationException(
          "Accumulator must be registered before send to executor")
      }
      val copyAcc = copyAndReset()
      assert(copyAcc.isZero, "copyAndReset must return a zero value copy")
      copyAcc.metadata = metadata
      copyAcc
    } else {
      this
    }
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    if (atDriverSide) {
      atDriverSide = false

      // Automatically register the accumulator when it is deserialized with the task closure.
      // This is for external accumulators and internal ones that do not represent task level
      // metrics, e.g. internal SQL metrics, which are per-operator.
      val taskContext = TaskContext.get()
      if (taskContext != null) {
        taskContext.registerAccumulator(this)
      }
    } else {
      atDriverSide = true
    }
  }

  override def toString: String = {
    if (metadata == null) {
      "Un-registered Accumulator: " + getClass.getSimpleName
    } else {
      getClass.getSimpleName + s"(id: $id, name: $name, value: $value)"
    }
  }
}


/**
 * An internal class used to track accumulators by Spark itself.
 */
private[spark] object AccumulatorContext {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  private val originals = new ConcurrentHashMap[Long, jl.ref.WeakReference[AccumulatorV2[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  /**
   * Returns a globally unique ID for a new [[AccumulatorV2]].
   * Note: Once you copy the [[AccumulatorV2]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /** Returns the number of accumulators registered. Used in testing. */
  def numAccums: Int = originals.size

  /**
   * Registers an [[AccumulatorV2]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[AccumulatorV2]] with the same ID was already registered, this does nothing instead
   * of overwriting it. We will never register same accumulator twice, this is just a sanity check.
   */
  def register(a: AccumulatorV2[_, _]): Unit = {
    originals.putIfAbsent(a.id, new jl.ref.WeakReference[AccumulatorV2[_, _]](a))
  }

  /**
   * Unregisters the [[AccumulatorV2]] with the given ID, if any.
   */
  def remove(id: Long): Unit = {
    originals.remove(id)
  }

  /**
   * Returns the [[AccumulatorV2]] registered with the given ID, if any.
   */
  def get(id: Long): Option[AccumulatorV2[_, _]] = {
    Option(originals.get(id)).map { ref =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      val acc = ref.get
      if (acc eq null) {
        throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
      acc
    }
  }

  /**
   * Clears all registered [[AccumulatorV2]]s. For testing only.
   */
  def clear(): Unit = {
    originals.clear()
  }

  // Identifier for distinguishing SQL metrics from other accumulators
  private[spark] val SQL_ACCUM_IDENTIFIER = "sql"
}


/**
 * An [[AccumulatorV2 accumulator]] for computing sum, count, and averages for 64-bit integers.
 *
 * @since 2.0.0
 */
class LongAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  private var _sum = 0L
  private var _count = 0L

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  override def isZero: Boolean = _sum == 0L && _count == 0

  override def copy(): LongAccumulator = {
    val newAcc = new LongAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  override def add(v: jl.Long): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  def add(v: Long): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Returns the number of elements added to the accumulator.
   * @since 2.0.0
   */
  def count: Long = _count

  /**
   * Returns the sum of elements added to the accumulator.
   * @since 2.0.0
   */
  def sum: Long = _sum

  /**
   * Returns the average of elements added to the accumulator.
   * @since 2.0.0
   */
  def avg: Double = _sum.toDouble / _count

  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue

  override def value: jl.Long = _sum
}


/**
 * An [[AccumulatorV2 accumulator]] for computing sum, count, and averages for double precision
 * floating numbers.
 *
 * @since 2.0.0
 */
class DoubleAccumulator extends AccumulatorV2[jl.Double, jl.Double] {
  private var _sum = 0.0
  private var _count = 0L

  override def isZero: Boolean = _sum == 0.0 && _count == 0

  override def copy(): DoubleAccumulator = {
    val newAcc = new DoubleAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0.0
    _count = 0L
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  def add(v: Double): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Returns the number of elements added to the accumulator.
   * @since 2.0.0
   */
  def count: Long = _count

  /**
   * Returns the sum of elements added to the accumulator.
   * @since 2.0.0
   */
  def sum: Double = _sum

  /**
   * Returns the average of elements added to the accumulator.
   * @since 2.0.0
   */
  def avg: Double = _sum / _count

  override def merge(other: AccumulatorV2[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Double): Unit = _sum = newValue

  override def value: jl.Double = _sum
}


/**
 * An [[AccumulatorV2 accumulator]] for collecting a list of elements.
 *
 * @since 2.0.0
 */
class CollectionAccumulator[T] extends AccumulatorV2[T, java.util.List[T]] {
  private val _list: java.util.List[T] = new ArrayList[T]

  override def isZero: Boolean = _list.isEmpty

  override def copyAndReset(): CollectionAccumulator[T] = new CollectionAccumulator

  override def copy(): CollectionAccumulator[T] = {
    val newAcc = new CollectionAccumulator[T]
    newAcc._list.addAll(_list)
    newAcc
  }

  override def reset(): Unit = _list.clear()

  override def add(v: T): Unit = _list.add(v)

  def add(coll: java.util.List[T]): Unit = _list.addAll(coll)

  override def merge(other: AccumulatorV2[T, java.util.List[T]]): Unit = other match {
    case o: CollectionAccumulator[T] => _list.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.List[T] = _list.synchronized {
    java.util.Collections.unmodifiableList(new ArrayList[T](_list))
  }

  private[spark] def setValue(newValue: java.util.List[T]): Unit = {
    _list.clear()
    _list.addAll(newValue)
  }
}


private[spark] object DataCharacteristicsAccumulator {
  def merge[A, B](zero: B)(f: (B, B) => B)(s1: Map[A, B], s2: Map[A, B]): Map[A, B] = {
    s1 ++ s2.map{ case (k, v) => k -> f(v, s1.getOrElse(k, zero)) }
  }

  def weightedMerge[A](zero: Double, weightOfFirst: Double)
                         (s1: Map[A, Double], s2: Seq[(A, Double)]): Seq[(A, Double)] = {
    val weightedS1 = s1.map(pair => (pair._1, pair._2 * weightOfFirst))
    (
      weightedS1 ++
      s2.map{ case (k, v) => k -> (v * (1 - weightOfFirst) + weightedS1.getOrElse(k, zero)) }
    ).toSeq
  }

  def isWeightable[T]()(implicit mf: ClassTag[T]): Boolean =
    classOf[Weightable] isAssignableFrom mf.runtimeClass

  def className[T]()(implicit mf: ClassTag[T]): String =
    mf.runtimeClass.getCanonicalName
}


/**
  * @todo Fix.
  */
class WeightableDataCharacteristicsAccumulator extends DataCharacteristicsAccumulator {
  override def increase(pair: Product2[Any, Any]): Double = 1.0
}

class DataCharacteristicsAccumulator
extends AccumulatorV2[(Any, Double), Map[Any, Double]] with Logging {
  private var _map: mutable.Map[Any, Double] = new mutable.HashMap[Any, Double]()

  private val TAKE: Int =
    SparkEnv.get.conf.getInt("spark.data-characteristics.take", 4)
  private val HISTOGRAM_SCALE_BOUNDARY: Int =
    SparkEnv.get.conf.getInt("spark.data-characteristics.histogram-scale-boundary", 20)
  private val BACKOFF_FACTOR: Double =
    SparkEnv.get.conf.getDouble("spark.data-characteristics.backoff-factor", 2.0)
  private val DROP_BOUNDARY: Double =
    SparkEnv.get.conf.getDouble("spark.data-characteristics.drop-boundary", 0.001)
  private val HISTOGRAM_SIZE_BOUNDARY: Int =
    SparkEnv.get.conf.getInt("spark.data-characteristics.histogram-size-boundary", 100)
  private val HISTOGRAM_COMPACTION: Int =
    SparkEnv.get.conf.getInt("spark.data-characteristics.histogram-compaction", 60)
  /**
    * Rate in which records are put into the histogram.
    * Value represent that each n-th input is recorded.
    */
  private var _sampleRate: Double = 1.0
  def sampleRate: Double = _sampleRate
  /**
    * Size or width of the histogram, that is equal to the size of the map.
    */
  private var _width: Int = 0
  def width: Int = _width

  private var _recordsPassed: Long = 0
  def recordsPassed: Long = _recordsPassed

  private var _nextScaleBoundary: Int = HISTOGRAM_SCALE_BOUNDARY

  private var _version: Int = 0
  def version: Int = _version
  def incrementVersion(): Unit = { _version += 1 }

  def updateTotalSlots(totalSlots: Int): Unit = {
    histogramCompaction = Math.max(histogramCompaction, totalSlots * 2)
    logInfo(s"Updated histogram compaction level based on $totalSlots number" +
      s" of total slots, to $histogramCompaction.")
  }

  private var histogramCompaction = HISTOGRAM_COMPACTION

  def normalize(histogram: Map[Any, Double], normalizationParam: Long): Map[Any, Double] = {
    val normalizationFactor = normalizationParam * sampleRate
    histogram.mapValues(_ / normalizationFactor)
  }

  def increase(pair: Product2[Any, Any]): Double = 1.0

  override def isZero: Boolean = _map.isEmpty

  override def copy(): AccumulatorV2[(Any, Double), Map[Any, Double]] = {
    val newMap = new DataCharacteristicsAccumulator
    newMap._map ++ _map
    newMap
  }

  override def reset(): Unit = _map.clear()

  override def add(v: (Any, Double)): Unit = {
    val pair = v
    _recordsPassed += 1
    if (Math.random() <= _sampleRate) { // Decided to record the key.
      val updatedHistogram = _map + ((pair._1, {
          val newValue = _map.get(pair._1) match {
            case Some(value) => value + increase(pair)
            case None =>
              _width = _width + 1
              increase(pair)
          }
          newValue
        }
      ))
      // Decide if scaling is needed.
      if (_width >= _nextScaleBoundary) {
        _sampleRate = _sampleRate / BACKOFF_FACTOR
        _nextScaleBoundary += HISTOGRAM_SCALE_BOUNDARY
        val scaledHistogram =
          updatedHistogram
            .mapValues(x => x / BACKOFF_FACTOR)
            .filter(p => p._2 > DROP_BOUNDARY)

        // Decide if additional cut is needed.
        if (_width > HISTOGRAM_SIZE_BOUNDARY) {
          _width = histogramCompaction
          _nextScaleBoundary = _width + HISTOGRAM_SCALE_BOUNDARY
          _map = mutable.Map[Any, Double]() ++ scaledHistogram.toSeq.sortBy(-_._2).take(histogramCompaction).toMap
        } else {
          _map = mutable.Map[Any, Double]() ++ scaledHistogram
        }
      } else { // No need to cut the histogram.
        _map = mutable.Map[Any, Double]() ++ updatedHistogram
      }
    } // Else histogram does not change.
  }

  override def merge(other: AccumulatorV2[(Any, Double), Map[Any, Double]]): Unit = other match {
    case o: DataCharacteristicsAccumulator =>
      _width = o._width
      _recordsPassed = o._recordsPassed
      _sampleRate = o._sampleRate
      _version = o._version
      _map = o._map
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  /**
    * Defines the current value of this accumulator
    */
  override def value: Map[Any, Double] = {
    scala.collection.immutable.HashMap[Any, Double]() ++ _map
  }

  def setValue(newMap: mutable.Map[Any, Double]) = {
    _map = newMap
  }
}

abstract class Weightable {
  def complexity(): Int
}

class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T]) extends AccumulatorV2[T, R] {
  private[spark] var _value = initialValue  // Current value on driver

  override def isZero: Boolean = _value == param.zero(initialValue)

  override def copy(): LegacyAccumulatorWrapper[R, T] = {
    val acc = new LegacyAccumulatorWrapper(initialValue, param)
    acc._value = _value
    acc
  }

  override def reset(): Unit = {
    _value = param.zero(initialValue)
  }

  override def add(v: T): Unit = _value = param.addAccumulator(_value, v)

  override def merge(other: AccumulatorV2[T, R]): Unit = other match {
    case o: LegacyAccumulatorWrapper[R, T] => _value = param.addInPlace(_value, o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: R = _value
}

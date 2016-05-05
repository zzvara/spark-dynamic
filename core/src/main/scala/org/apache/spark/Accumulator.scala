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
 * A simpler value of [[Accumulable]] where the result type being accumulated is the same
 * as the types of elements being merged, i.e. variables that are only "added" to through an
 * associative and commutative operation and can therefore be efficiently supported in parallel.
 * They can be used to implement counters (as in MapReduce) or sums. Spark natively supports
 * accumulators of numeric value types, and programmers can add support for new types.
 *
 * An accumulator is created from an initial value `v` by calling
 * [[SparkContext#accumulator SparkContext.accumulator]].
 * Tasks running on the cluster can then add to it using the [[Accumulable#+= +=]] operator.
 * However, they cannot read its value. Only the driver program can read the accumulator's value,
 * using its [[#value]] method.
 *
 * The interpreter session below shows an accumulator being used to add up the elements of an array:
 *
 * {{{
 * scala> val accum = sc.accumulator(0)
 * accum: org.apache.spark.Accumulator[Int] = 0
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
 * @param countFailedValues whether to accumulate values from failed tasks
 * @tparam T result type
*/
@deprecated("use AccumulatorV2", "2.0.0")
class Accumulator[T] private[spark] (
    // SI-8813: This must explicitly be a private val, or else scala 2.11 doesn't compile
    @transient private val initialValue: T,
    override val param: AccumulatorParam[T],
    name: Option[String] = None,
    countFailedValues: Boolean = false)
  extends Accumulable[T, T](initialValue, param, name, countFailedValues)


/**
 * A simpler version of [[org.apache.spark.AccumulableParam]] where the only data type you can add
 * in is the same type as the accumulated value. An implicit AccumulatorParam object needs to be
 * available when you create Accumulators of a specific type.
 *
 * @tparam T type of value to accumulate
 */
@deprecated("use AccumulatorV2", "2.0.0")
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T): T = {
    addInPlace(t1, t2)
  }
}


@deprecated("use AccumulatorV2", "2.0.0")
object AccumulatorParam {

  // The following implicit objects were in SparkContext before 1.2 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, as there are duplicate codes in SparkContext for backward
  // compatibility, please update them accordingly if you modify the following implicit objects.

  @deprecated("use AccumulatorV2", "2.0.0")
  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double): Double = 0.0
  }

  @deprecated("use AccumulatorV2", "2.0.0")
  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int): Int = 0
  }

  @deprecated("use AccumulatorV2", "2.0.0")
  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long): Long = t1 + t2
    def zero(initialValue: Long): Long = 0L
  }

  @deprecated("use AccumulatorV2", "2.0.0")
  implicit object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float): Float = t1 + t2
    def zero(initialValue: Float): Float = 0f
  }

  // Note: when merging values, this param just adopts the newer value. This is used only
  // internally for things that shouldn't really be accumulated across tasks, like input
  // read method, which should be the same across all tasks in the same stage.
  @deprecated("use AccumulatorV2", "2.0.0")
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

  private[spark] class ShuffleWriteDataCharacteristicsAccumulatorParam
    extends DataCharacteristicsAccumulatorParam

  private[spark] class ShuffleReadDataCharacteristicsAccumulatorParam
    extends DataCharacteristicsAccumulatorParam

  /**
    * Accumulator parameter to record data characteristics in form of key-histograms.
    *
    * The implementation allows to set a cap for the histogram. Also, it samples
    * well for any data size, due to its back-off sampling nature. It is not sensitive
    * to early or late heavy items. It performs with a higher error rate when the
    * distribution is close to uniform.
    */
  private[spark] class DataCharacteristicsAccumulatorParam
    extends MapAccumulatorParam[Any, Double] with Serializable {
    private val TAKE: Int = 4
    private val HISTOGRAM_SCALE_BOUNDARY: Double = 20
    private val BACKOFF_FACTOR: Double = 2.0
    private val DROP_BOUNDARY: Double = 0.01
    private val HISTOGRAM_SIZE_BOUNDARY: Int = 100
    private val HISTOGRAM_COMPACTION: Int = 60
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

    private var _version: Int = 0
    def version: Int = _version
    def incrementVersion(): Unit = { _version += 1 }

    def normalize(histogram: Map[Any, Double], normalizationParam: Long): Map[Any, Double] = {
      val normalizationFactor = normalizationParam * sampleRate
      histogram.mapValues(_ / normalizationFactor)
    }

    override def addAccumulator(r: Map[Any, Double], t: Map[Any, Double]): Map[Any, Double] = {
      val key = t.toList.head._1
      _recordsPassed += 1
      if (Math.random() <= _sampleRate) { // Decided to record the key.
        val updatedHistogram = r + ((key,
          r.get(key) match {
            case Some(value) => value + 1.0
            case None =>
              _width = _width + 1
              _sampleRate
          }
        ))
        // Decide if scaling is needed.
        if (_width * _sampleRate >= HISTOGRAM_SCALE_BOUNDARY) {
          _sampleRate = _sampleRate / BACKOFF_FACTOR
          val scaledHistogram =
            updatedHistogram
              .mapValues(x => x / BACKOFF_FACTOR)
              .filter(pair => pair._2 > DROP_BOUNDARY)
          // Decide if additional cut is needed.
          if (_width > HISTOGRAM_SIZE_BOUNDARY) {
            _width = HISTOGRAM_COMPACTION
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
      DataCharacteristicsAccumulatorParam.merge(zero)(f)(s1, s2)
    }

    override def compact(t: Map[Any, Double]): Map[Any, Double] = {
      compact(t, TAKE)
    }

    def compact(t: Map[Any, Double], take: Int): Map[Any, Double] = {
      t.toSeq.sortBy(-_._2).take(take).toMap
    }
  }

  private[spark] object DataCharacteristicsAccumulatorParam {

    def merge[A, B](zero: B)(f: (B, B) => B)(s1: Map[A, B], s2: Map[A, B]): Map[A, B] = {
      s1 ++ s2.map{ case (k, v) => k -> f(v, s1.getOrElse(k, zero)) }
    }
  }
}

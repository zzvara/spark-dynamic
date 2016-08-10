
package org.apache.spark.examples.repartitioning

import org.apache.spark.util.DataCharacteristicsAccumulator

object Sampling {
  def main(args: Array[String]) {
    val accum = new DataCharacteristicsAccumulator()

    val d1 = System.nanoTime()

    val data = (1 to 20000000).map {
      i => Math.random()
    }

    val d2 = System.nanoTime()

    println((d2 - d1) / (1000 * 1000))

    val t1 = System.nanoTime()

    data.map {
      x => x
    }

    val t2 = System.nanoTime()

    println((t2 - t1) / (1000 * 1000))

    val t3 = System.nanoTime()

    data.foreach {
      x => accum.add(x -> 1.0)
    }

    val t4 = System.nanoTime()

    println((t4 - t3) / (1000 * 1000))


  }
}

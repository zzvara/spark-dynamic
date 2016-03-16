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

// scalastyle:off println
package org.apache.spark.examples.repartitioning

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Wordcount")

    val initialPartitions = 5

    val sc = new SparkContext(conf)
    val lines = sc.parallelize(exponentialSampling(400, initialPartitions), initialPartitions)
      .map(uniformSplit(_, initialPartitions, 4))
      .map( x => {
        Thread.sleep(100)
        (x, 1)
      })
      .groupByKey(5)
    lines.collect()
      .foreach(tup => println(tup))
  }

  def exponentialSampling(size: Int, width: Int): Seq[Long] = {
    (1 to size).map{
      x => Math.log(1 + (Math.exp(width) - 1) * Math.random())
    } map {
      Math.ceil(_).toLong
    }
  }

  def uniformSplit(point: Long, width: Int, splitSize: Int): Long = {
      Math.floor(Math.random() * splitSize).toLong * width + point
  }
}
// scalastyle:on println

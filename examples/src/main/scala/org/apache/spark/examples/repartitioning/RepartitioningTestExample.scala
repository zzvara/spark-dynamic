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

package org.apache.spark.examples.repartitioning

import org.apache.spark.{SparkConf, SparkContext}

object RepartitioningTestExample {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("RepartitionerTestExample")

    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0), 3)
      .map(x => {
        Thread.sleep(10)
        (x.toInt, "record")})
      .groupByKey()
//      .mapPartitionsWithIndex((x,y) => y.map(z => (z._1, x)))
//      .distinct()
//      .groupByKey()
//      .map(x => (x._1, x._2.size, x._2))
//      .filter(x => x._2 != 1)
//      .collect().foreach(println)
      .map(x => (x._1, x._2.size))
    lines.collect().sortBy(_._1).foreach(println)

    Thread.sleep(60 * 60 * 24 * 1000)
  }
}

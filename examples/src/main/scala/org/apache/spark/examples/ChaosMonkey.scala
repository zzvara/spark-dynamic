
package org.apache.spark.examples

import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Queue}

object ChaosMonkey extends Logging {
  type Options = Map[Symbol, Any]

  val configuration = new SparkConf().setAppName("Chaos Monkey")
  final val spark = new SparkContext(configuration)
  final val streaming = new StreamingContext(spark, Seconds(2))
  val incubator = ArrayBuffer[RDD[(Int, Double)]]()
  var options: Options = _

  def main(arguments: Array[String]): Unit = {
    options = parseArguments(arguments)

    options('jars).toString.split(",").foreach(spark.addJar)

    val numberOfBananas = options('numberOfBananas).toString.toInt
    val chanceOfNewBreed = options('chanceOfNewBreed).toString.toDouble
    val chanceOfProgress = options('chanceOfProgress).toString.toDouble
    val chanceOfKill = options('chanceOfKill).toString.toDouble
    val chanceOfJoin = options('chanceOfJoin).toString.toDouble
    val chanceOfCoGroupFour = options('chanceOfCoGroupFour).toString.toDouble
    val chanceOfCoGroupThree = options('chanceOfCoGroupThree).toString.toDouble
    val minimumNumberOfPartitions = options('minimumNumberOfPartitions).toString.toInt
    val maximumNumberOfPartitions = options('maximumNumberOfPartitions).toString.toInt
    val aggregateResurrectionSize = options('aggregateResurrectionSize).toString.toInt

    logInfo("Going to create at least one new breed.")
    incubator += newBreed(10000,
      getPartitions(maximumNumberOfPartitions, minimumNumberOfPartitions))

    val rddQueue = new Queue[RDD[Int]]()

    streaming
      .queueStream(rddQueue)
      .map(x => (x % 10, 1))
      .reduceByKey(_ + _)
      .print()

    streaming.start()

    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += spark.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(1000)
    }

    (0 until numberOfBananas).foreach { banana =>
      scala.math.random match {
        case x if x < chanceOfNewBreed =>
          logInfo("Going to create a new breed.")
          incubator += newBreed(10000, maximumNumberOfPartitions)
        case x if x < chanceOfProgress =>
          logInfo("Going to progress breed.")
          incubator += pickBreed()
            .reduceByKey(_ + _)
            .flatMap {
              aggregate =>
                (1 to (aggregate._2 * aggregateResurrectionSize).toInt).map {
                  i => (aggregate._1, scala.math.random)
                }
            }
        case x if x < chanceOfKill =>
          logInfo("Going to kill breed with a count.")
          val result = pickBreed().count()
        case x if x < chanceOfJoin =>
          logInfo("Going to join breeds.")
          incubator += pickBreed()
            .join(pickBreed())
            .map(joined => (joined._1, joined._2._1))
        case x if x < chanceOfCoGroupThree =>
          logInfo("Going to co-group 3 breeds.")
          incubator += pickBreed()
            .cogroup(pickBreed(), pickBreed())
            .flatMap {
              group => group._2._1.map { value => (group._1, value) }
            }
        case x if x < chanceOfCoGroupFour =>
          logInfo("Going to co-group 4 breeds.")
          incubator += pickBreed()
            .cogroup(pickBreed(), pickBreed(), pickBreed())
            .flatMap {
              group => group._2._1.map { value => (group._1, value) }
            }
        case _ =>
          logInfo("Monkey bored. Doing nothing special.")
      }
    }

    logInfo("Chaos Monkey tired of this crap.")

    Thread.sleep(60 * 60 * 24 * 1000)
  }

  def pickBreed(): RDD[(Int, Double)] = {
    val breedID = (scala.math.random * incubator.size).floor.toInt
    logInfo(s"Picking breed with ID $breedID.")
    incubator(breedID)
  }

  def getPartitions(max: Int, min: Int) = {
    ((math.random * (max - min)) + min).toInt
  }

  def newBreed(size: Int, numberOfPartitions: Int): RDD[(Int, Double)] = {
    logInfo(s"Creating new breed with $size number of records and $numberOfPartitions" +
      s" number of partitions")
    spark.parallelize(1 to size, numberOfPartitions).map { i =>
      (i, scala.math.random)
    }
  }

  def parseArguments(arguments: Array[String]): Options = {
    def nextOption(map : Options, list: List[String]) : Options = {
      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map
        case "--jars" :: value :: tail =>
          nextOption(map ++ Map('jars -> value), tail)
        case "--numberOfBananas" :: value :: tail =>
          nextOption(map ++ Map('numberOfBananas -> value), tail)
        case "--chanceOfNewBreed" :: value :: tail =>
          nextOption(map ++ Map('chanceOfNewBreed -> value), tail)
        case "--chanceOfProgress" :: value :: tail =>
          nextOption(map ++ Map('chanceOfProgress -> value), tail)
        case "--chanceOfKill" :: value :: tail =>
          nextOption(map ++ Map('chanceOfKill -> value), tail)
        case "--chanceOfJoin" :: value :: tail =>
          nextOption(map ++ Map('chanceOfJoin -> value), tail)
        case "--chanceOfCoGroupFour" :: value :: tail =>
          nextOption(map ++ Map('chanceOfCoGroupFour -> value), tail)
        case "--chanceOfCoGroupThree" :: value :: tail =>
          nextOption(map ++ Map('chanceOfCoGroupThree -> value), tail)
        case "--minimumNumberOfPartitions" :: value :: tail =>
          nextOption(map ++ Map('minimumNumberOfPartitions -> value), tail)
        case "--maximumNumberOfPartitions" :: value :: tail =>
          nextOption(map ++ Map('maximumNumberOfPartitions -> value), tail)
        case "--aggregateResurrectionSize" :: value :: tail =>
          nextOption(map ++ Map('aggregateResurrectionSize -> value), tail)
        case _ => map
      }
    }

    nextOption(Map(), arguments.toList)
  }
}
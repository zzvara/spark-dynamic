package org.apache.spark.examples.repartitioning.utils

import java.util.{Base64, Date}

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer
import scala.io.Source

object LogReader extends Logging {
  type PartitionHistogram = Seq[Double]
  type Options = Map[Symbol, Any]

  val partitionHistograms = scala.collection.mutable.HashMap[Int, ListBuffer[PartitionHistogram]]()
  val retentiveKeyHistograms = scala.collection.mutable.ListBuffer[Seq[(Any, Double)]]()
  val globalHistograms = scala.collection.mutable.ListBuffer[Seq[(Any, Double)]]()

  def main(arguments: Array[String]) {
    val options = parseOptions(arguments.toList)

    val logFilePath = options('logFilePath).toString

    logInfo(s"Processing started with log file(s) [$logFilePath].")

    val events =
      Source
        .fromFile(logFilePath)("Cp1252")
        .getLines()
        .filter(_.contains("|||"))
        .map(Event.apply[Any])
        .filter(_.isDefined)
        .map(_.get)
        .toSeq

    var lastTime = events.head.time
    events.foreach {
      event =>
        if (lastTime.after(event.time)) {
          throw new RuntimeException(s"Events are not ordered [last time: $lastTime," +
            s"current time: ${event.time}]!")
        }
      lastTime = event.time
    }

    events
      .foreach { event =>
        event.payload match {
          case ("partitionHistogram", streamID: Int, partitionHistogram) =>
            partitionHistograms.getOrElseUpdate(
              streamID, ListBuffer[PartitionHistogram]()
            ) += partitionHistogram.asInstanceOf[PartitionHistogram]
          case ("retentiveHistogram", streamID: Int, retentiveKeyHistogram)
          if streamID == 1 =>
            retentiveKeyHistograms += retentiveKeyHistogram.asInstanceOf[Seq[(Any, Double)]]
          case ("globalHistogram", streamID: Int, globalHistogram)
          if streamID == 1 =>
            globalHistograms += globalHistogram.asInstanceOf[Seq[(Any, Double)]]
          case _ =>
        }
      }

    logInfo(s"Number of retentive histograms for stream 1 is ${retentiveKeyHistograms.size}.")
    logInfo(s"Number of global histograms for stream 1 is ${globalHistograms.size}.")

    /**
      * Partition histograms.
      */
    partitionHistograms.foreach {
      stream =>
        logInfo(s"Relative size of the maximal partition to the ideal average for " +
                s"stream ${stream._1}:")
        val average =
          stream._2.map {
            histogram => {
              (histogram.max / histogram.sum) / (1.0d / histogram.size)
            }
          }.sum / stream._2.size
        logInfo(s"$average")

        logInfo(s"Average size of the maximal partition for stream ${stream._1}:")
        val averageMaximum = stream._2.map(_.max).sum / stream._2.size
        logInfo(s"$averageMaximum")

        logInfo(s"Average sizes of partitions for stream ${stream._1}:")
        (0 until stream._2.head.size).foreach { i =>
          val averageSize = stream._2.map(histogram => histogram(i)).sum / stream._2.size
          logInfo(s"$i = $averageSize")
        }
    }

    /**
      * Retentive and global histogram comparision.
      */
    retentiveKeyHistograms.zip(globalHistograms).foreach {
      case (retentiveHistogram, globalHistogram) =>
        val key = retentiveHistogram.head._1
        logInfo(s"The heaviest key in the retentive histogram [$key] " +
                s"can be found at the [${
                  globalHistogram.map(_._1).zipWithIndex.find(_._1 == key).getOrElse((key, -1))._2
                }] position.")
    }

    retentiveKeyHistograms.zip(globalHistograms).foreach {
      case (retentiveHistogram, globalHistogram) =>
        val key = globalHistogram.head._1
        logInfo(s"The heaviest key in the global histogram [$key] " +
          s"can be found at the [${
            retentiveHistogram.map(_._1).zipWithIndex.find(_._1 == key).getOrElse((key, -1))._2
          }] position.")
    }

    retentiveKeyHistograms.zip(globalHistograms).foreach {
      case (retentiveHistogram, globalHistogram) =>
        val picked = retentiveHistogram.drop(15).head
          logInfo(s"The 16th key in the retentive histogram is [${
            picked._1
          }], which is in the [${
            globalHistogram.map(_._1).zipWithIndex.find(_._1 == picked._1)
              .getOrElse((picked._1, -1))._2}] position in the global histogram.")
    }

    logInfo("Processing finished.")
  }

  def parseOptions(arguments: List[String]): Options = {
    def nextOption(map : Options, list: List[String]) : Options = {
      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map
        case "--logFilePath" :: value :: tail =>
          nextOption(map ++ Map('logFilePath -> value), tail)
        case option :: tail =>
          logError(s"Unknown option '$option'!")
          throw new IllegalArgumentException("Unknown option!")
      }
    }

    nextOption(Map[Symbol, Any](), arguments)
  }
}

case class Event[T](time: Date, payload: T)

object Event {
  def apply[T](loggedEvent: String): Option[Event[T]] = {
    try {
      val split = loggedEvent.split("""\|\|\|""")
      if (split.size != 2) {
        throw new RuntimeException("Could not split event log correctly!")
      }
      val decodedPayload = Base64.getDecoder.decode(split(1))
      val deserializedPayload = SerializationUtils.deserialize(decodedPayload).asInstanceOf[T]
      val time = new Date(Date.parse(split(0).take(17)))
      Some(new Event(time, deserializedPayload))
    } catch {
      case e: Exception => None
    }
  }
}
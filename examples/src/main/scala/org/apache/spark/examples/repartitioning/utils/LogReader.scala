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

  def main(arguments: Array[String]) {
    val options = parseOptions(arguments.toList)

    val logFilePath = options('logFilePath).toString

    logInfo(s"Processing started with log file [$logFilePath].")

    Source
      .fromFile(logFilePath)
      .getLines()
      .filter(_.contains("|||"))
      .map(Event.apply[Any])
      .foreach { event =>
        event.payload match {
          case ("partitionHistogram", streamID: Int, partitionHistogram) =>
            partitionHistograms.getOrElseUpdate(
              streamID, ListBuffer[PartitionHistogram]()
            ) += partitionHistogram.asInstanceOf[PartitionHistogram]
          case _ =>
        }
      }

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
  def apply[T](loggedEvent: String): Event[T] = {
    val split = loggedEvent.split("""\|\|\|""")
    if (split.size != 2) {
      throw new RuntimeException("Could not split event log correctly!")
    }
    val decodedPayload = Base64.getDecoder.decode(split(1))
    val deserializedPayload = SerializationUtils.deserialize(decodedPayload).asInstanceOf[T]
    val time = new Date(Date.parse(split(0).take(17)))
    new Event(time, deserializedPayload)
  }
}
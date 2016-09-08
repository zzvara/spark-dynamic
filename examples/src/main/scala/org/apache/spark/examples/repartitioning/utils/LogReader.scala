package org.apache.spark.examples.repartitioning.utils

import java.util.{Base64, Date}

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.PartitioningInfo
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer
import scala.io.Source

object LogReader extends Logging {
  type PartitionHistogram = Seq[Double]
  type Options = Map[Symbol, Any]

  val partitionHistograms = scala.collection.mutable.HashMap[Int, ListBuffer[PartitionHistogram]]()
  val partitionSizeFrequencies = scala.collection.mutable.HashMap[Int, Int]()
  val retentiveKeyHistograms = scala.collection.mutable.ListBuffer[Seq[(Any, Double)]]()
  val globalHistograms = scala.collection.mutable.ListBuffer[Seq[(Any, Double)]]()
  val partitioningInfos = scala.collection.mutable.ListBuffer[PartitioningInfo]()
  val sCutFrequencies = scala.collection.mutable.HashMap[Int, Int]()

  def main(arguments: Array[String]) {
    val options = parseOptions(arguments.toList)
    val histogramComparison = options('histogramComparison).toString.toBoolean
    val partitioningInfo = options('partitioningInfo).toString.toBoolean

    val logFilePath = options('logFilePath).toString
    val forcePartitionSize = options.get('forcePartitionSize).map(_.toString.toInt)

    logInfo(s"Processing started with log file(s) [$logFilePath].")

    Source
      .fromFile(logFilePath)(options('sourceFileEncoding).toString)
      .getLines()
      .filter(_.contains("|||"))
      .map(Event.apply[Any])
      .filter(_.isDefined)
      .map(_.get)
      .foreach { event =>
        event.payload match {
          case ("partitionHistogram", streamID: Int, partitionHistogram) =>
            partitionHistograms.getOrElseUpdate(
              streamID, ListBuffer[PartitionHistogram]()
            ) += partitionHistogram.asInstanceOf[PartitionHistogram]
          case ("retentiveHistogram", streamID: Int, retentiveKeyHistogram)
          if streamID == 1 && histogramComparison =>
            retentiveKeyHistograms += retentiveKeyHistogram.asInstanceOf[Seq[(Any, Double)]]
          case ("globalHistogram", streamID: Int, globalHistogram)
          if streamID == 1 && histogramComparison =>
            globalHistograms += globalHistogram.asInstanceOf[Seq[(Any, Double)]]
          case ("partitioningInfo", streamID: Int, info)
          if streamID == 1 && partitioningInfo =>
            partitioningInfos += info.asInstanceOf[PartitioningInfo]
          case _ =>
        }
      }

    logInfo(s"Number of retentive histograms for stream 1 is ${retentiveKeyHistograms.size}.")
    logInfo(s"Number of global histograms for stream 1 is ${globalHistograms.size}.")
    logInfo(s"Number of partitioning infos for stream 1 is ${partitioningInfos.size}.")

    /**
      * Validating partition histograms.
      */
    partitionHistograms.foreach {
      stream => stream._2 foreach {
        histogram => require((histogram.sum - 1.0).abs < 0.01, "Partition histogram is not valid!")
      }
    }
    logInfo("If you see this message, the partition histograms are valid.")

    /**
      * Partitioning information.
      */
    partitioningInfos.foreach {
      info => sCutFrequencies.update(
        info.sCut,
        sCutFrequencies.getOrElse(info.sCut, 0) + 1
      )
    }
    logInfo("Number of s-cuts and their frequencies for stream 1 is the following:")
    sCutFrequencies.foreach {
      frequency => logInfo(s"The s-cut ${frequency._1} appears ${frequency._2} times.")
    }

    /**
      * Partition histograms.
      */
    partitionHistograms.foreach {
      stream =>
        logInfo(s"Relative size of the maximal partition to the ideal average for " +
                s"stream ${stream._1}:")
        val histograms = forcePartitionSize match {
          case Some(size) =>
            logWarning(s"Forcing partition histogram sizes to be $size!")
            stream._2.map(_.take(size))
          case _ => stream._2
        }
        val average =
          histograms.map {
            partitionHistogram => {
              partitionSizeFrequencies.update(
                partitionHistogram.size,
                partitionSizeFrequencies.getOrElse(partitionHistogram.size, 0) + 1
              )
              (partitionHistogram.max / partitionHistogram.sum) / (1.0d / partitionHistogram.size)
            }
          }.sum / histograms.size
        logInfo(s"$average")

        logInfo(s"Average size of the maximal partition for stream ${stream._1}:")
        val averageMaximum = histograms.map(_.max).sum / histograms.size
        logInfo(s"$averageMaximum")

        logInfo(s"Average sizes of partitions for stream ${stream._1}:")
        val to = histograms.head.size
        (0 until to).foreach { i =>
          val averageSize = histograms
            .filter(_.size == to)
            .map(histogram => histogram(i)).sum / histograms.size
          logInfo(s"$i = $averageSize")
        }
    }

    /**
      * Partition size frequencies.
      */
    logInfo("Partition size frequencies are the following:")
    partitionSizeFrequencies.foreach {
      frequency => logInfo(s"[Partition size ${frequency._1} occurred ${frequency._2} times.]")
    }

    if (histogramComparison) {
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
              .getOrElse((picked._1, -1))._2
          }] position in the global histogram.")
      }
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
        case "--histogramComparison" :: value :: tail =>
          nextOption(map ++ Map('histogramComparison -> value), tail)
        case "--sourceFileEncoding" :: value :: tail =>
          nextOption(map ++ Map('sourceFileEncoding -> value), tail)
        case "--forcePartitionSize" :: value :: tail =>
          nextOption(map ++ Map('forcePartitionSize -> value), tail)
        case "--partitioningInfo" :: value :: tail =>
          nextOption(map ++ Map('partitioningInfo -> value), tail)
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
  private var split: Array[String] = _
  private var decodedPayload: Array[Byte] = _
  private var deserializedPayload: Any = _
  private var time: Date = _

  def apply[T](loggedEvent: String): Option[Event[T]] = {
    try {
      split = loggedEvent.split("""\|\|\|""")
      if (split.length != 2) {
        throw new RuntimeException("Could not split event log correctly!")
      }
      decodedPayload = Base64.getDecoder.decode(split(1))
      deserializedPayload = SerializationUtils.deserialize(decodedPayload)
      time = new Date(Date.parse(split(0).take(17)))
      Some(new Event(time, deserializedPayload.asInstanceOf[T]))
    } catch {
      case e: Exception => None
    }
  }
}
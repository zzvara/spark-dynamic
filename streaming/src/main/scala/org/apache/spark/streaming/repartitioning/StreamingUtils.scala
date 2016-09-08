package org.apache.spark.streaming.repartitioning

import org.apache.spark.streaming.dstream.{DStream, ShuffledDStream, Stream}

import scala.collection.mutable

object StreamingUtils {
  private val streamIDToDStream = mutable.HashMap[Int, DStream[_]]()
  private val streamIDToStream = mutable.HashMap[Int, Stream]()
  private val streamIDToChildren = mutable.HashMap[Int, Seq[DStream[_]]]()

  def initialize(outputDStreams: Seq[DStream[_]]): Unit = {
    outputDStreams.foreach(ds => {
      streamIDToDStream.update(ds.id, ds)
      ds.dependencies.foreach(dep => {
        val children = streamIDToChildren.getOrElseUpdate(dep.id, Seq.empty[DStream[_]])
        streamIDToChildren.update(dep.id, children :+ ds)
      })
      initialize(ds.dependencies)
    })

  }

  def updateStreamIDToStreamMap(streamId: Int, stream: Stream) = {
    streamIDToStream.update(streamId, stream)
  }

  def getDStream(streamId: Int): DStream[_] = {
    streamIDToDStream.get(streamId) match {
      case Some(ds) => ds
      case None => throw new RuntimeException(s"Cannot find DStream for id $streamId")
    }
  }

  def getChildren(streamId: Int): Seq[DStream[_]] = {
    streamIDToChildren.get(streamId) match {
      case Some(children) => children
      case None => throw new RuntimeException(s"Cannot find children DStreams for id $streamId")
    }
  }

  def getParents(dStream: ShuffledDStream[_, _, _]): Seq[Stream] = {
    dStream.dependencies.map(dep => streamIDToStream(dep.id))
  }

  /**
    * @todo Finds only one shuffle head.
    */
  def getShuffleHead(dStream: DStream[_]): Option[ShuffledDStream[_, _, _]] = {
    def findShuffleHead(dStreams: Seq[DStream[_]]): Option[ShuffledDStream[_, _, _]] = {
      if (dStreams.nonEmpty) {
        dStreams.find(stream => stream.isInstanceOf[ShuffledDStream[_, _, _]]) match {
          case Some(sds) => Some(sds.asInstanceOf[ShuffledDStream[_, _, _]])
          case None => findShuffleHead(dStreams.flatMap(_.dependencies))
        }
      } else {
        None
      }
    }
    findShuffleHead(Seq(dStream))
  }
}
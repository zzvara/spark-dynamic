package org.apache.spark.streaming.repartitioning

import org.apache.spark.streaming.dstream.{DStream, InternalMapWithStateDStream, ShuffledDStream}

import org.apache.spark.streaming.dstream.{DStream, InternalMapWithStateDStream, ShuffledDStream, Stream}

import scala.collection.mutable

object StreamingUtils {
  val streamIDToDStream: mutable.HashMap[Int, DStream[_]] = mutable.HashMap[Int, DStream[_]]()
  val streamIDToStream: mutable.HashMap[Int, Stream] = mutable.HashMap[Int, Stream]()
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
      case None => Seq.empty[DStream[_]]
    }
  }

  def getParents(dStream: DStream[_]): Seq[Stream] = {
    dStream.dependencies.map(dep => streamIDToStream(dep.id))
  }

  /**
    * @todo Finds only one shuffle head.
    */
  def getShuffleHead(dStream: DStream[_]): Option[DStream[_]] = {
    def findShuffleHead(dStreams: Seq[DStream[_]]): Option[DStream[_]] = {
      if (dStreams.nonEmpty) {
        dStreams.find(stream => stream.isInstanceOf[ShuffledDStream[_, _, _]]
          || stream.isInstanceOf[InternalMapWithStateDStream[_, _, _, _]]) match {
          case Some(sds) => Some(sds)
          case None => findShuffleHead(dStreams.flatMap(_.dependencies))
        }
      } else {
        None
      }
    }

    findShuffleHead(Seq(dStream))
  }

  def printChildrenTree(streamId: Int): Unit = {
    val children = getChildren(streamId)
    children.foreach(ch => println(s"$streamId -> ${ch.id}"))
    children.foreach(ch => printChildrenTree(ch.id))
  }

  def printParentsTree(streamId: Int): Unit = {
    val dStream = streamIDToDStream(streamId)
    dStream match {
      case sds: ShuffledDStream[_, _, _] =>
        val parents = getParents(sds)
        parents.foreach(par => println(s"$streamId -> ${par.ID}"))
        parents.foreach(par => printParentsTree(par.ID))
      case _ => println(s"Cannot print parents of DStream $streamId, because it is not a ShuffledDStream.")
    }
  }
}
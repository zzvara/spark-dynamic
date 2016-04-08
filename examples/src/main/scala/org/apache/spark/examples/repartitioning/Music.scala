
package org.apache.spark.examples.repartitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.reflect.ClassTag
import scala.util.parsing.json.JSON

object Music {
  type M[T] = Map[String, T]

  def drop(probability: Double): Boolean = {
    if (Math.random() <= probability) {
      false
    } else {
      true
    }
  }

  def loadData[T: ClassTag](
    fileName: String,
    constructor: (Int, Int, Map[String, Any], Option[Map[String, Any]]) => T,
    dropProbability: Double = 0.0)(implicit context: SparkContext): RDD[T] = {
    context
      .textFile(fileName, 38)
      // .filter(x => drop(probability = dropProbability))
      .map(_.split("\t").drop(1))
      .flatMap(a => {
        Idomaar.parse(a) match {
          case Some((id: Int, created, properties, linked)) =>
            List(constructor(id, created, properties, linked))
          case None => List.empty
        }
      })
  }

  def main(args: Array[String]) {
    val configuration = new SparkConf()
      .setAppName("Music")
        .setJars(Seq("/home/ehnalis/Projects/dyna/assembly/target/scala-2.11/jars/spark-assembly_2.11-2.0.0-SNAPSHOT.jar",
          "/home/ehnalis/Projects/dyna/examples/target/scala-2.11/jars/spark-examples_2.11-2.0.0-SNAPSHOT.jar"))

    implicit val context = new SparkContext(configuration)

    /**
      * Loading some data...
      */

    val tracks =
      loadData(args(0), Track.apply)

    val tags =
      loadData(args(1), Tag.apply)

    /*

    val users =
      loadData(args(0), User.apply, 0.9)

    val loves =
      loadData(args(3), Love.apply)

    println(s"Number of users: ${users.count()}")
    val sampleUsers = users.take(5)
    sampleUsers foreach {
      x => println(x.name)
    }
    */

    val tagsWithTracks =
      tracks
        .flatMap {
          t => {
            if (t.tags != null) {
              t.tags
                .flatMap( _.get("id"))
                .map(_.asInstanceOf[Double].toInt)
                .map(ID => (ID, "track payload"))
            } else {
              Iterator.empty
            }
          }
        }
    tagsWithTracks
      .groupByKey()
      .map(x => x)
      .count()
    /*
        .mapPartitionsWithIndex((x,y) => y.map(z => (z._1, x)))
        .distinct()
        .groupByKey()
        .map(x => (x._1, x._2.size, x._2))
        .filter(x => x._2 != 1)
        .collect().take(10).foreach(println)
        */

    /*
    val tagFrequencies = tagsWithTracks
      .map(pair => (pair._1, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .cache()

    println(tagFrequencies.count())
    tagFrequencies.take(50) foreach println

    tagsWithTracks.join {
      tags.map(tag => (tag.ID, tag.value))
    }
    .map(x => {
        x
      }
    )
    .count()
    */

    /*
    println(s"Number of tracks: ${tracks.count()}")
    val sampleTracks = tracks.take(5)
    sampleTracks foreach {
      x => println(x.name)
    }

    println(s"Number of loves: ${loves.count()}")
    val lovesSample = loves.take(10)
    lovesSample foreach {
      _.subjects.headOption.map {
        _.get("id").map {
          println
        }
      }
    }

    users.map {
      u => (u.ID, u)
    }.join(
      loves.map {
        l => (l.objects.filter(_.contains("id")).head.getOrElse("id", -1.0).asInstanceOf[Double].toInt, l)
      })
    .count()
    */



    /** file: tracks.small.idomaar
      * (205245,10214)
      * (70618,5366)
      * (189631,4774)
      * (115355,4321)
      * (11056,3916)
      * (154891,3523)
      * (81223,3452)
      * (122769,3253)
      * (117167,2936)
      * (11957,2367)
      * (84597,2251)
      * (107401,2097)
      * (46208,2054)
      * (227682,2042)
      * (11242,2042)
      * (195173,1962)
      * (54087,1811)
      * (76913,1744)
      * (3982,1696)
      * (218701,1684)
      * (103394,1684)
      * (43212,1675)
      *
      *
      * file: tracks.idomaar
      * (205245,201299)
      * (70618,107996)
      * (189631,97575)
      * (115355,85636)
      * (11056,77362)
      * (154891,68905)
      * (81223,68711)
      * (122769,64637)
      * (117167,58396)
      * (11957,46915)
      * (84597,45545)
      * (107401,42515)
      * (11242,40848)
      * (227682,40481)
      * (46208,39912)
      * (195173,39339)
      * (54087,36935)
      * (76913,35318)
      * (218701,34799)
      * (3982,34732)
      * (103394,34260)
      * (226723,33744)
      * (43212,33418)
      * (115752,31700)
      * (50604,31009)
      * (193464,29160)
      * (198998,27844)
      * (31015,27212)
      * (105199,26899)
      * (70625,26783)
      * (107398,26064)
      * (122106,25772)
      * (6120,24930)
      * (116047,24518)
      * (103055,24265)
      * (57528,23303)
      * (195456,22884)
      * (186445,21709)
      * (50247,21249)
      * (201327,20731)
      * (92799,20514)
      * (194264,20172)
      * (4425,20164)
      * (29723,19910)
      * (89467,19771)
      * (3668,18886)
      * (109806,18362)
      * (144192,17889)
      * (83064,17742)
      * (35060,17592)
      */

    Thread.sleep(60 * 60 * 24 * 1000)
  }
}

abstract class Idomaar(val ID: Int, val created: Int) extends Serializable

object Idomaar {
  def parse(parts: Array[String]):
  Option[(Int, Int, Map[String, Any], Option[Map[String, Any]])] = {
    val linked = parts.length match {
      case 4 => parseMap(parts(3))
      case 3 => None
    }
    val map = parseMap(parts(2))
    map match {
      case Some(m) => Some((parts(0).toInt, parts(1).toInt, m, linked))
      case None => None
    }
  }

  private def parseMap(s: String) = {
    val json: Option[Any] = JSON.parseFull(s)
    json match {
      case Some(m) => Some(m.asInstanceOf[Map[String, Any]])
      case None => None
    }
  }
}

trait Base[T]{
  def apply(ID: Int, created: Int,
            map: Map[String, Any],
            linked: Option[Map[String, Any]])(implicit m: Manifest[T]): T = {
    m.erasure.getConstructors
      .filter(_.getParameterCount == 4)
      .head.newInstance(ID: Integer, created: Integer, map, linked).asInstanceOf[T]
  }
}

object Gender extends Enumeration {
  type Gender = Value
  val Male, Female, Unknown = Value

  def apply(s: String): Gender.Gender = {
    if (s == "m") Male
    else if (s == "f") Female
    else Unknown
  }
}

class User(
            override val ID: Int,
            override val created: Int,
            val name: String,
            val gender: Gender.Gender,
            val age: Int,
            val country: String,
            val play_count: Int,
            val playlists: Int,
            val subscriber_type: String) extends Idomaar(ID, created) {
  def this(ID: Int, created: Int, map: Map[String, Any], linked: Option[Map[String, Any]]) {
    this(ID,
      created,
      map("lastfm_username").asInstanceOf[String],
      Gender(map("gender").asInstanceOf[String]),
      map("age").asInstanceOf[Double].toInt,
      map("country").asInstanceOf[String],
      map("playcount").asInstanceOf[Double].toInt,
      map("playlists").asInstanceOf[Double].toInt,
      map("subscribertype").asInstanceOf[String])
  }
}

object User {
  def apply(ID: Int, created: Int,
            map: Map[String, Any],
            linked: Option[Map[String, Any]]): User = {
    new User(ID, created, map, linked)
  }
}

class Track(
             override val ID: Int,
             override val created: Int,
             val duration: Int,
             val play_count: Int,
             val mbid: String,
             val name: String,
             val artists: List[Map[String, Any]],
             val albums: List[Map[String, Any]],
             val tags: List[Map[String, Any]])
  extends Idomaar(ID, created) {
  def this(ID: Int, created: Int, map: Map[String, Any], linked: Option[Map[String, Any]]) {
    this(ID,
      created,
      map.getOrElse("duration", 0.0).asInstanceOf[Double].toInt,
      map("playcount").asInstanceOf[Double].toInt,
      map.getOrElse("mbid", "").asInstanceOf[String],
      map.getOrElse("name", "").asInstanceOf[String],
      linked.map { _.getOrElse("artists", List(Map[String, Any]())).asInstanceOf[List[Map[String, Any]]] }.getOrElse(List(Map[String, Any]())),
      linked.map { _.getOrElse("albums", List(Map[String, Any]())).asInstanceOf[List[Map[String, Any]]] }.getOrElse(List(Map[String, Any]())),
      linked.map { _.getOrElse("tags", List(Map[String, Any]())).asInstanceOf[List[Map[String, Any]]] }.getOrElse(List(Map[String, Any]())))
  }
}

object Track {
  def apply(ID: Int, created: Int,
            map: Map[String, Any],
            linked: Option[Map[String, Any]]): Track = {
    new Track(ID, created, map, linked)
  }
}

class Love(
            override val ID: Int,
            override val created: Int,
            val value: String,
            val subjects: List[Map[String, Any]],
            val objects: List[Map[String, Any]])
  extends Idomaar(ID, created) {
  def this(ID: Int, created: Int, map: Map[String, Any], linked: Option[Map[String, Any]]) {
    this(ID,
      created,
      map.getOrElse("value", "").asInstanceOf[String],
      linked.map { _.get("subjects").get.asInstanceOf[List[Map[String, Any]]] }.getOrElse(List(Map[String, Any]())),
      linked.map { _.get("objects").get.asInstanceOf[List[Map[String, Any]]] }.getOrElse(List(Map[String, Any]())))
  }
}

object Love extends Base[Love]{
}

class Tag(
           override val ID: Int,
           override val created: Int,
           val value: String,
           val url: String)
  extends Idomaar(ID, created) {
  def this(ID: Int, created: Int, map: Map[String, Any], linked: Option[Map[String, Any]]) {
    this(ID,
      created,
      map.getOrElse("value", "").asInstanceOf[String],
      map.getOrElse("url", "").asInstanceOf[String])
  }
}

object Tag {
  def apply(ID: Int, created: Int,
            map: Map[String, Any],
            linked: Option[Map[String, Any]]): Tag = {
    new Tag(ID, created, map, linked)
  }
}
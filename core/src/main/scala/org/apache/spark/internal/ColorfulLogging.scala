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

package org.apache.spark.internal

import java.io.{FileInputStream, FileNotFoundException}
import java.util.Properties
import javax.lang.model.SourceVersion

import org.apache.spark.internal.ColorfulLogging._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

// Tracks:
// Dynamic repartitioning:
// -"DRCommunication"
// -"DRHistogram"
// -"DRRepartitioner"
// -"DRRepartitioning"
// -"DRDebug"

/**
  * Colored loggers must be defined in log4j-defaults.properties!
  *
  * Predefined logging tracks: "black", "grey", "red", "yellow", "green", "cyan", "blue", "magenta",
  * "strongBlack", "strongGrey", "strongRed", "strongYellow",
  * "strongGreen", "strongCyan", "strongBlue", "strongMagenta",
  * "default", "strongDefault", "root"
  */
trait ColorfulLogging extends Logging {

  private val loggers = mutable.Map[String, Logger]()
  if (isColorfulLoggingOn) {
    colors.foreach(c => loggers.update(c, null))
    strongColors.foreach(c => loggers.update(c, null))
  }

  private def getLogger(color: String): Logger = {
    loggers.get(color) match {
      case Some(l) => l
      case None =>
        throw new RuntimeException(s"Color '$color.' is not supported in colorful logging mode")
    }
  }

  private def updateLogger(color: String, logger: Logger) = {
    loggers.update(color, logger)
    logger
  }

  private def logName(color: String) = {
    // Ignore trailing $'s in the class names for Scala objects
    s"colors.$color." + this.getClass.getName.stripSuffix("$")
  }

  private def log(color: String): Logger = {
    if (color == ROOT) {
      log
    } else {
      var logg = getLogger(color)
      if (logg == null) {
        initializeLogIfNecessary(false)
        logg = updateLogger(color, LoggerFactory.getLogger(logName(color)))
      }
      logg
    }
  }

  protected def logInfo(msg: => String, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logInfo(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isInfoEnabled) logg.info(msg)
      }
    }
  }

  protected def logDebug(msg: => String, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logDebug(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isDebugEnabled) logg.debug(msg)
      }
    }
  }

  protected def logTrace(msg: => String, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logTrace(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isTraceEnabled) logg.trace(msg)
      }
    }
  }

  protected def logWarning(msg: => String, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logWarning(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isWarnEnabled) logg.warn(msg)
      }
    }
  }

  protected def logError(msg: => String, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logError(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isErrorEnabled) logg.error(msg)
      }
    }
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logInfo(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isInfoEnabled) logg.info(msg, throwable)
      }
    }
  }

  protected def logDebug(msg: => String, throwable: Throwable, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logDebug(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isDebugEnabled) logg.debug(msg, throwable)
      }
    }
  }

  protected def logTrace(msg: => String, throwable: Throwable, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logTrace(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isTraceEnabled) logg.trace(msg, throwable)
      }
    }
  }

  protected def logWarning(msg: => String, throwable: Throwable, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logWarning(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isWarnEnabled) logg.warn(msg, throwable)
      }
    }
  }

  protected def logError(msg: => String, throwable: Throwable, track: String, tracks: String*) {
    val trackSet = tracks.toSet + track
    trackSet.foreach(validateTrackName)
    if (!isColorfulLoggingOn) {
      if (logToRootLogger || trackSet.contains(ROOT)) {
        logError(msg)
      }
    } else {
      var colorSet = resolveTracks(trackSet)
      if (logToRootLogger && colorSet.isEmpty) colorSet += ROOT

      for (color <- colorSet) {
        val logg = log(color)
        if (logg.isErrorEnabled) logg.error(msg, throwable)
      }
    }
  }
}


object ColorfulLogging extends Logging {

  private val ROOT = "root"
  private val propertiesPath = "conf/colorful-logging.properties"
  private val properties = getProperties

  private val isColorfulLoggingOn = properties.getProperty("isColorfulLoggingOn", "false").toBoolean
  private val logToRootLogger = properties.getProperty("logToRootLogger", "true").toBoolean
  private val tracksToColors = mutable.Map[String, mutable.Set[String]]()
  private val suppressedColors = mutable.Set[String]()
  private val colors = Set[String]("default", "black", "grey", "red",
                                   "yellow", "green", "cyan", "blue", "magenta")
  private val strongColors = colors.map("strong" + _.capitalize)
  private val colorsAndRoot = colors ++ strongColors + ROOT

  if (isColorfulLoggingOn) {
    parseSuppressedColors()
    for (color <- colors) {
      parseTracksForColor(color)
    }
    parseTracksForRoot()
    logInfo(s"Track-color assignment: $tracksToColors")
  }

  private def parseSuppressedColors(): Unit = {
    val s = properties.getProperty("suppressed", "")
    if (s.nonEmpty) {
      s.split(",").toSet[String].map(_.trim).foreach { (x: String) =>
        if (!colorsAndRoot.contains(x)) {
          throw new RuntimeException(s"'$x' is not a valid color name to suppress")
        }
        suppressedColors.add(x)
        if(x != ROOT) {
          suppressedColors.add("strong" + x.capitalize)
        }
      }
    }
  }

  private def parseTracksForColor(color: String): Unit = {
    if (!suppressedColors.contains(color)) {
      val strongColor = "strong" + color.capitalize
      val s = properties.getProperty(color, "")
      if (s.nonEmpty) {
        s.split(",").toSet[String].map(_.trim).foreach { (x: String) =>
          if (x.charAt(0) == '!') {
            val y = x.substring(1, x.length)
            if (!SourceVersion.isName(y) || colorsAndRoot.contains(y)) {
              throw new RuntimeException(s"'$y' is not a valid track name for color $color")
            }
            tracksToColors.get(y) match {
              case Some(cs) =>
                cs.remove(color)
                cs.add(strongColor)
              case None => tracksToColors.update(y, mutable.Set[String](strongColor))
            }
          } else {
            if (!SourceVersion.isName(x) || colorsAndRoot.contains(x)) {
              throw new RuntimeException(s"'$x' is not a valid track name for color $color")
            }
            tracksToColors.get(x) match {
              case Some(cs) => if (!cs.contains(strongColor)) cs.add(color)
              case None => tracksToColors.update(x, mutable.Set[String](color))
            }
          }
        }
      }
    }
  }

  private def parseTracksForRoot(): Unit = {
    if (!suppressedColors.contains(ROOT)) {
      val s = properties.getProperty(ROOT, "")
      if (s.nonEmpty) {
        s.split(",").toSet[String].map(_.trim).foreach { (x: String) =>
          if (x.isEmpty) throw new RuntimeException(s"Empty trace name is invalid for root")
          if (!SourceVersion.isName(x) || colorsAndRoot.contains(x)) {
            throw new RuntimeException(s"'$x' is not a valid track name for color root")
          }
          tracksToColors.get(x) match {
            case Some(cs) => cs.add(ROOT)
            case None => tracksToColors.update(x, mutable.Set[String](ROOT))
          }
        }
      }
    }
  }

  private def getProperties: Properties = {
    val properties: Properties = new Properties()
    try {
      val in = new FileInputStream(propertiesPath)
      properties.load(in)
      in.close()
    } catch {
      case t: FileNotFoundException =>
        logWarning("Could not load colorful logging properties file.")
    }
    properties
  }

  private def resolveTracks(tracks: Set[String]): Set[String] = {
    tracks.flatMap(t => if (suppressedColors.contains(t)) {
      Set[String]()
    } else if (colorsAndRoot.contains(t)) {
      Set[String](t)
    } else {
      tracksToColors.getOrElse(t, mutable.Set[String]())
    })
  }

  private def validateTrackName(track: String): Unit = {
    if (!colorsAndRoot.contains(track) && !SourceVersion.isName(track)) {
      throw new RuntimeException(s"'$track' is not a valid track name")
    }
  }
}
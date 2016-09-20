package org.apache.spark.repartitioning.core

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging

class Configuration {
  private val typesafeConfiguration = ConfigFactory.load("repartitioning.conf")

  def internal: Config = typesafeConfiguration
}

object Configuration extends Logging {
  private var _configuration: Option[Configuration] = None

  def get(): Configuration = {
    _configuration.getOrElse {
      logInfo("Creating configuration for the first time.")
      _configuration = Some(new Configuration)
      _configuration.get
    }
  }

  def internal(): Config = {
    get().internal
  }

  def getOption: Option[Configuration] = {
    _configuration
  }
}
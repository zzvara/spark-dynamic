package org.apache.spark.examples.streaming.twitter

import java.util

import org.apache.commons.lang3.SerializationUtils
import org.apache.kafka.common.serialization.Deserializer
import twitter4j.Status

class TweetDeserializer extends Deserializer[Status] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): Status = {
    SerializationUtils.deserialize(data).asInstanceOf[Status]
  }
}

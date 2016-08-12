
package org.apache.spark.examples.streaming.twitter

import java.util

import org.apache.commons.lang.SerializationUtils
import org.apache.kafka.common.serialization.Serializer
import twitter4j.Status

class TweetSerializer() extends Serializer[Status] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: Status): Array[Byte] = {
    SerializationUtils.serialize(data)
  }

  override def close(): Unit = {}
}

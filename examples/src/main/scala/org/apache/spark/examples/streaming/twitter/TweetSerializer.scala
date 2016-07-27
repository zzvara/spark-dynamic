
package org.apache.spark.examples.streaming.twitter

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import org.apache.commons.lang.SerializationUtils
import twitter4j.Status

class TweetSerializer(props: VerifiableProperties = null) extends Encoder[Status] {
  override def toBytes(t: Status): Array[Byte] = {
    SerializationUtils.serialize(t)
  }
}

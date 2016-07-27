
package org.apache.spark.examples.streaming.twitter

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.commons.lang.SerializationUtils
import twitter4j.Status

class StatusDecoder(props: VerifiableProperties = null) extends Decoder[Status] {
  override def fromBytes(bytes: Array[Byte]): Status = {
    SerializationUtils.deserialize(bytes).asInstanceOf[Status]
  }
}

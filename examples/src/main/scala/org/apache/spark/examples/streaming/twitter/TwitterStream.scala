
package org.apache.spark.examples.streaming.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStream {
  def main(args: Array[String]): Unit = {

    val cfg = new SparkConf()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(cfg, Seconds(2))
    ssc.checkpoint(args(4))
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    stream
      .flatMap(tweet => tweet.getHashtagEntities.map(tag => (tag.getText, (tag.getText, tweet))))
      .groupByKey()
      .map {
        group => group.ensuring(
          _._2.map(_._1).forall(x => group._1 == x), "False!")
      }
      .count()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

package com.ltxhxpdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingKafkaReceiverDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(4))
    val inputStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, Config.zkQuorum, Config.grpupId, Config.topics)

    inputStream.flatMap { case (key, value) => {
      value.split(" ")
    }
    }.map((_, 1)).reduceByKey(_ + _).print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}

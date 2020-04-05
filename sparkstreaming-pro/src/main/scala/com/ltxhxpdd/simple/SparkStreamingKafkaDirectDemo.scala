package com.ltxhxpdd.simple

import com.ltxhxpdd.Config
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingKafkaDirectDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(4))

    val topic: Set[String] = Set("test1")
    val inputStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, Config.kafkaParams, topic)
    inputStream.flatMap { case (key, value) => value.split(" ") }.map((_, 1)).reduceByKey(_ + _).print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

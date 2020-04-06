package com.ltxhxpdd.keyfunc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val batchInterval = 2
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingUSB")
    val ssc = new StreamingContext(conf, Seconds(batchInterval))
    ssc.checkpoint("file:///D:\\checkpoint")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("10.50.100.67", 2222)
    val pairs: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1))

    /** *
      * def reduceByKeyAndWindow(
      * reduceFunc: (V, V) => V,
      * windowDuration: Duration,
      * slideDuration: Duration,
      * partitioner: Partitioner
      */

    pairs.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(batchInterval * 3), Seconds(batchInterval * 2)).print()

    ssc.start()
    ssc.awaitTermination()

  }

}

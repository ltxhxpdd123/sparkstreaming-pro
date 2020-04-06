package com.ltxhxpdd.keyfunc

import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingUSB")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("file:///D:\\checkpoint")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("10.50.100.67", 2222)
    val pairs: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1))

    //    pairs.updateStateByKey(updateFunc).print()
    pairs.updateStateByKey((nowValueList: Seq[Int], history: Option[Int]) => Option(nowValueList.sum + history.getOrElse(0))).print()
    ssc.start()
    ssc.awaitTermination()


    /***
      *
      * @param nowValueList:当前dstream中的value的seq
      * @param history：状态保存，之前的历史的一个状态，类型可能与当前的不一样
      * @return
      */
    def updateFunc(nowValueList: Seq[Int], history: Option[Int]) = {
      Option(nowValueList.sum + history.getOrElse(0))
    }
  }

}

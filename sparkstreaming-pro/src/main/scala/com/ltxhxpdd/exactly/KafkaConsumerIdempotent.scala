package com.ltxhxpdd.exactly

import java.sql.DriverManager

import com.ltxhxpdd.zookeeper.CuratorUtils
import com.ltxhxpdd.{Config, SparkOffsetUtil}
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumerIdempotent {
  private val curator: CuratorFramework = CuratorUtils.curatorClient

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val kafkaParams: Map[String, String] = Config.kafkaParams
    val groupId: String = Config.grpupId
    val messages = SparkOffsetUtil.createMsg(ssc, kafkaParams, Set(Config.topic), groupId, curator)


    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map(x => x._2).foreachPartition(partition => {
          val dbConn = DriverManager.getConnection(Config.jdbcUrl, Config.jdbcUser, Config.jdbcPassword)
          partition.foreach(msg => {
            if (msg.contains(",")) {
              val name = msg.split(",")(0)
              val orderid = msg.split(",")(1)
              val sql = s"insert into myorders(name, orderid) values ('$name', '$orderid') on DUPLICATE KEY UPDATE NAME='${name}'"
              val pstmt = dbConn.prepareStatement(sql)
              pstmt.execute()
            }
          })
          dbConn.close()
        })
        SparkOffsetUtil.store(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupId, curator)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

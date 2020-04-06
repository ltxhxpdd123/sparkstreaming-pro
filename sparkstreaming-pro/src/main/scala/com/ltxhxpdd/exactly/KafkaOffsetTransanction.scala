package com.ltxhxpdd.exactly



import com.ltxhxpdd.{Config, SparkOffsetUtil}
import scalikejdbc._
import com.ltxhxpdd.zookeeper.CuratorUtils
import com.mysql.jdbc.Driver
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object KafkaOffsetTransanction {
  private val curator: CuratorFramework = CuratorUtils.curatorClient

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val kafkaParams: Map[String, String] = Config.kafkaParams
    val groupId: String = Config.grpupId

    val driverClass: String = classOf[Driver].getName
    // 设置连接池
    ConnectionPool.singleton(Config.jdbcUrl, Config.jdbcUser, Config.jdbcPassword)


    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select topic, partid, offset from mytopic".
        map { r =>
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
        }.list.apply().toMap
    }

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    messages.foreachRDD(rdd =>{
      if (!rdd.isEmpty()){
        rdd.foreachPartition(partition => {
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val pOffsetRange: OffsetRange = offsetRanges(TaskContext.get.partitionId)

          /** 自带事务 **/
          DB.localTx { implicit session =>
            partition.foreach(msg => {
              // 或者使用scalike的batch 插入
              val name = msg._2.split(",")(0)
              val id = msg._2.split(",")(1)
              val dataResult = sql"""insert into  mydata(name,id) values (${name},${id})""".execute().apply()
            })
            //                    val ret = 1 / 0
            val offsetResult =
              sql"""update mytopic set offset = ${pOffsetRange.untilOffset} where topic =
                  ${pOffsetRange.topic} and partid = ${pOffsetRange.partition}""".update.apply()
          }

        })
      }
    })



  }

}

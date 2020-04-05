package com.ltxhxpdd.zookeeper

import com.ltxhxpdd.Config
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.{JavaConversions, mutable}

object SparkStreamingDirectWithZooPersit {

  private val client: CuratorFramework = CuratorUtils.curatorClient

  def getKafkaConsumerOffset(streamingContext: StreamingContext, kafkaParams: Map[String, String], topic: String, groupId: String) = {
    //第一步：从zk中获取对应的偏移量
    val offsets: mutable.Map[TopicAndPartition, Long] = scala.collection.mutable.Map[TopicAndPartition, Long]()
    //1,参考receiver的dir的层级关系自定义的:/mykafka/offsets/${topic}/${group}/${partition}
    val curatorClient: CuratorFramework = client
    val path = s"offsets/${topic}/${groupId}"
    //2,检验这个目录是否存在：验证当前路劲是否存在，如果不存在，创建之
    if (curatorClient.checkExists().forPath(path) == null) {
      //当前节点不存在
      curatorClient.create().creatingParentsIfNeeded().forPath(path)
    }
    //3,经过checkExists之后，当前目录parent一定是存在的
    val partitions: mutable.Buffer[String] = JavaConversions.asScalaBuffer(curatorClient.getChildren.forPath(path))
    for (partition <- partitions) {
      val partitionPath = s"${path}/${partition}"
      val offset = new String(curatorClient.getData.forPath(partitionPath)).toLong
      offsets.put(TopicAndPartition(topic, partition.toInt), offset)
    }
    offsets.toMap
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ltxhxpdd")
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(2))

    /** 第一步：从zk中获取topic的相应group的offset **/

    val kafkaParams: Map[String, String] = Config.kafkaParams
    val groupId: String = Config.grpupId
    val partitionToLong: Map[TopicAndPartition, Long] = getKafkaConsumerOffset(streamingContext, kafkaParams, Config.topic, groupId)

    var inputStream: InputDStream[(String, String)] = null
    if (partitionToLong.isEmpty) {
      //从zk中获取的偏移量为null，第一次操作
      inputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, Set(Config.topic))
    } else {
      /** 从指定的offset中拿 **/
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
      inputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, partitionToLong, messageHandler)
    }

    inputStream.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty()) {
        println("-------------------------------------------")
        println(s"Time: $time")
        println("############rdd'count: " + rdd.count()
          /** 看看当前这个RDD有几条数据 **/)
        println("-------------------------------------------")
        /** 第三步：保存偏移量 **/
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offsetRange <- offsetRanges) {
          val topic = offsetRange.topic
          val partition = offsetRange.partition
          val path = s"offsets/${topic}/${groupId}/${partition}"
          if ( client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path)
          }
          client.setData().forPath(path, ("" + offsetRange.untilOffset).getBytes())
        }

      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

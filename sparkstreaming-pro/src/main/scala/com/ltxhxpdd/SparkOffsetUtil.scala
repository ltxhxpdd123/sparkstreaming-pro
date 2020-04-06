package com.ltxhxpdd

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils, OffsetRange}

import scala.collection.{JavaConversions, mutable}

object SparkOffsetUtil {
  def createMsg(ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String], group: String, curator: CuratorFramework): InputDStream[(String, String)] = {
    //1、从zk中读取对应的偏移量
    val offsets: Map[TopicAndPartition, Long] = getFromOffsets(topics, group, curator)
    var message: InputDStream[(String, String)] = null
    //2、偏移量是否存在
    if (offsets.isEmpty) {
      //读取到的偏移量为null
      message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
      //不为空
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
      message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc,
        kafkaParams,
        offsets,
        messageHandler
      )
    }
    message
  }

  /** *
    * 添加check的kafka中实际的偏移量，防止kafka过期清理数据,造成OutOfRange
    *
    * @param group
    * @param kafkaParam
    * @param offsets
    */
  def checkOffset(group: String, kafkaParam: Map[String, String], offsets: mutable.Map[TopicAndPartition, Long]) = {
    val cluster: KafkaCluster = new KafkaCluster(kafkaParam)
    /** Either[A+, B+]--->代表要A成立，要么B成立，换句话说要么A有值，要么B有值，二选一 **/
    val either: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = cluster.getEarliestLeaderOffsets(offsets.keySet.toSet)
    if (either.isRight) {
      val rightMap: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = either.right.get
      for ((tap, leaderOffset) <- rightMap) {
        val topic = tap.topic
        val partition = tap.partition
        val earliestOffset: Long = leaderOffset.offset
        val zkOffset = offsets(tap) //获取zk中的偏移量
        if (zkOffset < earliestOffset) {
          //如果zk中的偏移量信息小于了kafka中的偏移量信息，将zk中的偏移量替换为kafka的earliestOffset
          offsets.put(tap, earliestOffset)
        }
      }
    }
  }

  def getFromOffsets(topics: Set[String], group: String, curator: CuratorFramework): Map[TopicAndPartition, Long] = {
    var offsets = mutable.Map[TopicAndPartition, Long]()
    for (topic <- topics) {
      val path = s"offsets/${topic}/${group}"
      //验证路劲是否存在
      checkExists(path, curator)
      for (partition <- JavaConversions.asScalaBuffer(curator.getChildren.forPath(path))) {
        val fullPath = s"${path}/${partition}"
        val data = curator.getData.forPath(fullPath)
        //byte[]
        val offset = new String(data).toLong
        offsets.put(TopicAndPartition(topic, partition.toInt), offset)
      }
    }

    /** 解决偏移量不一致的问题 **/
    checkOffset(group, kafkaParam, offsets)
    offsets.toMap
  }

  def checkExists(path: String, curator: CuratorFramework) {
    if (curator.checkExists().forPath(path) == null) {
      curator.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  def store(offsetRanges: Array[OffsetRange], group: String, curator: CuratorFramework): Unit = {
    for (offsetRange <- offsetRanges) {
      val partition = offsetRange.partition
      val topic = offsetRange.topic
      val offset = offsetRange.untilOffset
      val path = s"offsets/${topic}/${group}/${partition}"
      checkExists(path, curator)
      curator.setData().forPath(path, (offset + "").getBytes())
    }
  }

}

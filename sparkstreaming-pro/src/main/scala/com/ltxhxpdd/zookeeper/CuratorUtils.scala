package com.ltxhxpdd.zookeeper

import com.ltxhxpdd.Config
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

object CuratorUtils {

  val curatorClient = {
    val client = CuratorFrameworkFactory.builder()
      .connectString(Config.zkQuorum)
      .namespace("mykafka")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
    client.start()
    client
  }

}

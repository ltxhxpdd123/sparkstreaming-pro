package com.ltxhxpdd.zookeeper

import java.util

import com.ltxhxpdd.Config
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.collection.{JavaConversions, mutable}

object CuratorTest {
  def main(args: Array[String]): Unit = {


    val client = CuratorFrameworkFactory.builder()
      .connectString(Config.zkQuorum)
      .namespace("lantian-curator-test")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
    client.start()

//    client.create().creatingParentsIfNeeded().forPath("lantian", "2020-04-05".getBytes())
    val paths: mutable.Buffer[String] = JavaConversions.asScalaBuffer(client.getChildren.forPath(""))
    for (path <- paths) {
      val data: String = new String(client.getData.forPath(s"/${path}"))
      println(data)
    }

    client.close()
  }

}

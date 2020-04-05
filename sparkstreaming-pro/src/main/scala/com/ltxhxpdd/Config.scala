package com.ltxhxpdd

object Config {

  val zkQuorum: String = "10.50.100.66:2181,10.50.100.67:2181,10.50.100.68:2181"
  val grpupId: String = "test1"
  val topics: Map[String, Int] = Map("test1" -> 3)
  val topic = "test1"

  val kafkaParams = Map(
    "bootstrap.servers" -> "10.50.100.66:9092,10.50.100.67:9092,10.50.100.68:9092",
    "group.id" -> "test1",
    "auto.offset.reset" -> "smallest"
  )


}

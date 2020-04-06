package com.ltxhxpdd.keyfunc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SomeTransformationOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("_01SparkTransformationOps")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    mapFunc(sc)
//    flatMapFunc(sc)
    gbkMethod(sc)
  }

  def mapFunc(sc: SparkContext) = {
    val list = 1 to 10
    val listRDD: RDD[Int] = sc.parallelize(list)
    println("listRDD's partition: " + listRDD.getNumPartitions)
    val retRdd: RDD[Int] = listRDD.map { num => num * 10 }
    retRdd.foreach(x => println(x))
  }

  def flatMapFunc(sc: SparkContext) = {
    val list = List(
      "hello you",
      "hello  me he",
      "you you"
    )
    val flatRdd: List[String] = list.flatMap { case line => line.split(" ") }
    flatRdd.foreach(println)
  }

  def gbkMethod(sc: SparkContext) = {
    val value: RDD[String] = sc.textFile("G:\\workspace\\sparkstreaming-pro\\sparkstreaming-pro\\src\\main\\scala\\com\\ltxhxpdd\\keyfunc\\gbk.txt")
    val mapRdd: RDD[(String, String)] = value.map(line => {
      val data: Array[String] = line.split("\t")
      val key = data(0) + "_" + data(1)
      val value = data(2)
      (key, value)
    })
    val result: RDD[(String, Iterable[String])] = mapRdd.groupByKey()
    result.foreach { case (key, value) => {
      println(key + ":" + value.toList.toString())
    }
    }
  }

}

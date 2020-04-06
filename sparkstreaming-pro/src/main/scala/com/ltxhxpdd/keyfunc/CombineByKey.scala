package com.ltxhxpdd.keyfunc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 使用combineByKey来模拟reduceByKey **/

/** *
  * def combineByKey[C](
  * createCombiner: V => C,
  * mergeValue: (C, V) => C,
  * mergeCombiners: (C, C) => C,
  * numPartitions: Int): RDD[(K, C)] = self.withScope {
  */

object CombineByKey {

  def createCombiner(value: Int) = {
    (value, 1)
  }

  def mergeValue(acc0: (Int, Int), num: Int) = {
    (acc0._1 + num, acc0._2 + 1)
  }

  def mergeCombiners(acc0: (Int, Int), acc1: (Int, Int)) = {
    (acc0._1 + acc1._1, acc0._2 + acc1._2)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("_02SparkTransformationOps")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val init_data = Array(("coffee", 1), ("coffee", 2), ("panda", 3), ("coffee", 9))
    val data = sc.parallelize(init_data, 2)
    val resultRdd: RDD[(String, (Int, Int))] = data.combineByKey(createCombiner, mergeValue, mergeCombiners)
    val retult: RDD[(String, Double)] = resultRdd.map { case (key, valueTuple) => {
      (key, (valueTuple._1 / valueTuple._2).toDouble)
    }
    }
    retult.foreach { case (key, value) => {
      println(key + "===》" + value)
    }
    }
    sc.stop()
  }

}

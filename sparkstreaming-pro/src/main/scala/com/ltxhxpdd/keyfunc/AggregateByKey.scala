package com.ltxhxpdd.keyfunc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://www.jianshu.com/p/851b85d6c62f
  * aggregateByKey()() 根据key聚合
  */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("_02SparkTransformationOps")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("b", 3), ("a", 3), ("b", 4), ("a", 5)), 2)

    //("a",1),("b",2),("b",3) =>("a",1),("b",3)
    //("a",3),("b",4),("a",5) =>("b",4),("a",5)
    val resultRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    resultRdd.collect().foreach(println)



    sc.stop()
  }

}

package com.zcl.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount3 {
  def main(args: Array[String]): Unit = {
    //》》》》》》建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //》》》》》》执行业务

    //1读取数据，获取一行一行文件
    val lines: RDD[String] = sc.textFile("datas")

    //扁平化，拆分成一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))


    val wordToOne = words.map(
      word => (word, 1)
    )

    //spark框架将分组和聚合通过一个方法实现
    //reduceByKey相同的key对value做聚合
    val count = wordToOne.reduceByKey(_ + _)
    count.foreach(print)  //(hello,4)(world,2)(spark,2)

    //》》》》》关闭连接
    sc.stop()
  }
}

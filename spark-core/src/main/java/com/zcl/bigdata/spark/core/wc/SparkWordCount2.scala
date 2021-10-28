package com.zcl.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount2 {
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

    val wordGroup = wordToOne.groupBy(
      t => t._1
    )

    val count = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }
    val arr = count.collect()
    arr.foreach(print)    //(hello,4)(world,2)(spark,2)


    //》》》》》关闭连接
    sc.stop()
  }
}

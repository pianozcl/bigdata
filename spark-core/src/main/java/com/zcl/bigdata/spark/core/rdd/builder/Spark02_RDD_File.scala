package com.zcl.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //创建RDD，将文件的数据作为数据源
    //val rdd = sc.textFile("datas/1.txt")

    //打印该目录下所有文件
    //val rdd = sc.textFile("datas")


    //使用通配符，打印该目录下开头为1的文件
    val rdd = sc.textFile("datas/1*.txt")
    rdd.collect().foreach(println)


    //读取的结果为元组，第一个元素表示文件路径，第二个表示文件内容
    /**
      * (file:/Users/chenliangzhou/Desktop/personal-notes/bigdata/datas/11.txt,hello world
      * hello spark
      * hello 11)
      * (file:/Users/chenliangzhou/Desktop/personal-notes/bigdata/datas/2.txt,hello world
      * hello spark)
      * (file:/Users/chenliangzhou/Desktop/personal-notes/bigdata/datas/1.txt,hello world
      * hello spark)
      */
    val rdd1 = sc.wholeTextFiles("datas")
    rdd1.collect().foreach(println)
  }
}

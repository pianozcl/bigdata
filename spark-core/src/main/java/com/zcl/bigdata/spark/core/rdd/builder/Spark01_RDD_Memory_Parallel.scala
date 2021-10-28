package com.zcl.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Parallel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //创建RDD，将内存的数据作为数据源
    val seq = Seq[Int](1,2,3,4)

    //第二个参数代表分区的数量
    val rdd = sc.makeRDD(seq, 2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}

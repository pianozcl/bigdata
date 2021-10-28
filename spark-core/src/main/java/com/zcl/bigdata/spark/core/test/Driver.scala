package com.zcl.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket


/**
  * 分布式计算
  * 将数据集1，2，3，4拆分。分别交给两个不同的executor执行。
  * executor最后计算结果为2，4
  * executor2最后计算结果为6，8
  */
object Driver {
  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost",9999)
    val client2 = new Socket("localhost",8888)

    val out = client.getOutputStream
    val objOut = new ObjectOutputStream(out)
    val dataSet = new DataSet()
    val task = new Task()
    task.logic = dataSet.logic
    task.datas = dataSet.datas.take(2)
    objOut.writeObject(task)
    objOut.flush()
    objOut.close()
    client.close()


    val out2 = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)
    val task2 = new Task()
    task2.logic = dataSet.logic
    task2.datas = dataSet.datas.takeRight(2)
    objOut2.writeObject(task2)
    objOut2.flush()
    objOut2.close()
    client2.close()
  }
}

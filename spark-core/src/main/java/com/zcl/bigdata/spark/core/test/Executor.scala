package com.zcl.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务端启动，等待数据")

    val socket = server.accept()
    val in = socket.getInputStream
    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[Task]
    val ints = task.compute()
    println("计算[9999]结果" + ints)  //计算[9999]结果List(2, 4)
    in.close()
    socket.close()
  }
}

package com.zcl.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor2 {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(8888)
    println("服务端启动，等待数据")

    val socket = server.accept()
    val in = socket.getInputStream
    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[Task]
    val ints = task.compute()
    println("计算[8888]结果" + ints)  //计算[8888]结果List(6, 8)
    in.close()
    socket.close()
  }
}

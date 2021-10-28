package com.zcl.bigdata.spark.core.test

class Task extends Serializable {
  var datas: List[Int] = null
  var logic: Int => Int = null

  def compute() = {
    datas.map(logic)
  }
}

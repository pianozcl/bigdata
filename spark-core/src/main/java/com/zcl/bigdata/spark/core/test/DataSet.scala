package com.zcl.bigdata.spark.core.test

class DataSet extends Serializable {
  val datas = List(1,2,3,4)
  val logic = (x:Int) => {x * 2}

  def compute() = {
    datas.map(logic)
  }
}

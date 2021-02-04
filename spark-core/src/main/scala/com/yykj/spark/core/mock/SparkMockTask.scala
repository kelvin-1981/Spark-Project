package com.yykj.spark.core.mock

class SparkMockTask extends Serializable {

  /**
   * 数据
   */
  var datas : List[Int] = List(1,2,3)

  /**
   * 处理逻辑
   * 声明logic的类型(函数)  var logic : (Int) => Int
   */
  //val logic = (num : Int) => { num * 2 }
  var logic : (Int) => Int = {_ * 2}

  /**
   * 计算
   */
  def compute() : List[Int] = {
    datas.map(logic)
  }
}

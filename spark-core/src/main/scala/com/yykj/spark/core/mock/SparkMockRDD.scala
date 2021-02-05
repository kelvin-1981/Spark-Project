package com.yykj.spark.core.mock

import scala.collection.mutable

/**
 * Spark RDD:数据结构 最小计算单元（进行组合） 用于封装全量数据及应用逻辑
 *
 */
class SparkMockRDD extends Serializable {

  /**
   * 数据 : _代表空值
   */
  var datas: List[Int] = _

  /**
   * 处理逻辑
   * 声明logic的类型(函数)  var logic : (Int) => Int
   */
  //val logic = (num : Int) => { num * 2 }
  //var logic : (Int) => Int = {_ * 2}
  //_代表空值
  var logic: (Int) => Int = _
}

package com.yykj.spark.core.mock

/**
 * Partition:分区对象 负责存储分区数据信息
 */
class SparkMockPartition extends Serializable {

  /**
   * 分区编码
   */
  var parition_index : Int = _

  /**
   * 当前分区数据
   */
  var parition_data : Seq[Int] = _
}

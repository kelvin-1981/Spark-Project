package com.yykj.spark.core.mock

import scala.collection.mutable

/**
 * Paritioner:Spark 分区器 负责对数据进行分区,分配数据
 */
class SparkMockParitioner {

  /**
   * 分区数量
   */
  private val partition_num : Int = 2

  /**
   * 全部数据
   */
   var datas_all : List[Int] = _

  /**
   * 创建分区列表
   */
  def createParitiones() : Array[SparkMockPartition] = {
    // TODO: 判断条件
    if (datas_all == null) null

    // TODO: 生成task列表
    var split_num = datas_all.length / partition_num
    var partitiones: mutable.Queue[SparkMockPartition] = new mutable.Queue[SparkMockPartition]()

    var data_start = 0
    var data_end = 0
    for (num <- 0 to partition_num - 1) {
      // TODO: 模拟分区
      data_start = data_start + (split_num * num)
      if(data_start < 0){
        data_start = 0
      }
      data_end = split_num * (num + 1) - 1
      if (data_end + split_num > datas_all.length - 1){
        data_end = datas_all.length - 1
      }

      var data_len : Int = data_end - data_start + 1
      var task_datas: Array[Int] = new Array[Int](data_len)
      var arr_index : Int = 0
      for(index <- data_start to data_end){
        task_datas(arr_index) = datas_all(index)
        arr_index = arr_index + 1
      }

      // TODO:生成Executor
      val partition = new SparkMockPartition
      partition.parition_order = num
      partition.parition_data = task_datas.toArray
      partitiones += partition
    }

    partitiones.toArray
  }
}

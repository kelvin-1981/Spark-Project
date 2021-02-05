package com.yykj.spark.core.mock

import scala.collection.mutable

/**
 * Spark RDD:
 * 1.具备弹性：存储、容错、计算、分区
 * 2.分布式：数据存储在分布式大数据平台 RDD进行分布式计算；
 * 3.计算：RDD是Spark最小计算单元，可以通过RDD组合完成丰富功能
 * 4.数据模型：用于封装计算逻辑，不保存数据
 * 5.抽象类
 * 6.不可变：RDD封装了计算逻辑，不可以改变。如需新计算逻辑需要编写新的RDD
 * 7.可分区：实现并行计算
 */
class SparkMockRDD extends Serializable {

  /**
   * 分区数量
   */
  var partition_num : Int = 2

  /**
   * 数据 : _代表空值
   * Spark RDD 不保存数据
   */
  var datas: List[Int] = _

  /**
   * 处理逻辑:Spark RDD 计算逻辑不可改变
   * 声明logic的类型(函数)  var logic : (Int) => Int
   */
  //val logic = (num : Int) => { num * 2 }
  //var logic : (Int) => Int = {_ * 2}
  //_代表空值
  val logic: (Int) => Int = {_ * 100}

  /**
   * 生成分区
   */
  def splitPartition(): mutable.Queue[SparkMockTask] = {

    // TODO: 判断条件
    if (datas == null || logic == null || partition_num <= 0) {
      null
    }

    // TODO: 生成subtask列表
    var split_num = datas.length / partition_num
    var partition_list: mutable.Queue[SparkMockTask] = new mutable.Queue[SparkMockTask]()

    var data_start = 0
    var data_end = 0
    for (num <- 0 to partition_num - 1) {
      // TODO: 模拟分区
      data_start = data_start + (split_num * num)
      if(data_start < 0){
        data_start = 0
      }
      data_end = split_num * (num + 1) - 1
      if (data_end + split_num > datas.length - 1){
        data_end = datas.length - 1
      }

      var data_len : Int = data_end - data_start + 1
      var task_datas: Array[Int] = new Array[Int](data_len)
      var arr_index : Int = 0
      for(index <- data_start to data_end){
        task_datas(arr_index) = datas(index)
        arr_index = arr_index + 1
      }

      // TODO:生成Executor
      var task = new SparkMockTask
      task.logic = logic
      task.datas = task_datas.toList
      partition_list += task
    }

    partition_list
  }
}

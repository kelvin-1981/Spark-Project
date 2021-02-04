package com.yykj.spark.core.mock

import scala.collection.mutable

class SparkMockTask extends Serializable {

  /**
   * executor数量
   */
  val num_executor = 2

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

  /**
   *
   */
  def createSubTaskes(): mutable.Queue[SparkMockSubTask] = {
    // TODO: 判断条件
    if (datas == null || logic == null) {
      null
    }

    // TODO: 生成subtask列表
    var split_num = datas.length / num_executor
    var task_list: mutable.Queue[SparkMockSubTask] = new mutable.Queue[SparkMockSubTask]()

    var data_start = 0
    var data_end = 0
    for (num <- 0 to num_executor - 1) {
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
      var sub_datas: Array[Int] = new Array[Int](data_len)
      var arr_index : Int = 0
      for(index <- data_start to data_end){
        sub_datas(arr_index) = datas(index)
        arr_index = arr_index + 1
      }
      sub_datas.map(println)

      // TODO:生成Executor
      var subTask = new SparkMockSubTask
      subTask.logic = this.logic
      subTask.datas = sub_datas.toList
      task_list += subTask
    }

    task_list
  }
}

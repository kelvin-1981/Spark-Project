package com.yykj.spark.core.mock

import java.io.ObjectOutputStream
import java.net.Socket
import scala.collection.mutable

object SparkMockDriver {

  /**
   * 模拟Driver：Diriver与Executor的通讯过程
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.待计算数据及业务规则
    var datas : List[Int] = List(1,2,3,4,5)
    var logic : (Int) => Int = {_ * 10}

    // TODO: 2.创建作业信息,自定义数据及处理逻辑
    val rdd = new SparkMockRDD()
    rdd.datas = datas
    rdd.logic = logic

    // TODO: 3.切片生成executor task
    var taskes : mutable.Queue[SparkMockTask] = createTaskes(rdd)
    if(taskes.length <= 0){
      println("Driver(客户端)生成Executor Tasks失败...")
    }
    else{
      println("Driver(客户端)生成Executor Tasks成功...")
    }

    // TODO: 4.发送数据
    var client : Socket = null

    for(num <- 0 to taskes.length - 1){
      // TODO: 连接服务器
      if(num == 0){
        client = new Socket("localhost", 9999)
      }
      else{
        client = new Socket("localhost", 8888)
      }
      println("Driver(客户端)连接Executor-" + (num + 1) + "成功...")

      // TODO: 发送数据
      val os = client.getOutputStream
      val obj_os = new ObjectOutputStream(os)
      obj_os.writeObject(taskes(num))
      obj_os.flush()
      os.close()
      obj_os.close()
      client.close()
      println("Driver(客户端)向Executor-" + (num + 1) + "发送数据成功...")
    }

    // TODO: 4.通知
    print("Driver(客户端)数据发送完毕...")
  }

  /**
   * 生成task任务集合
   */
  def createTaskes(rdd: SparkMockRDD): mutable.Queue[SparkMockTask] = {

    var datas = rdd.datas
    var logic = rdd.logic
    var num_executor = 2

    // TODO: 判断条件
    if (datas == null || logic == null || num_executor <= 0) {
      null
    }

    // TODO: 生成subtask列表
    var split_num = datas.length / num_executor
    var task_list: mutable.Queue[SparkMockTask] = new mutable.Queue[SparkMockTask]()

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
      task_list += task
    }

    task_list
  }
}

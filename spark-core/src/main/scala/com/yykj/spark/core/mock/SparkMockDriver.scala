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
    // Spark RDD 计算逻辑不可改变，不可外部传递计算逻辑
    //var logic : (Int) => Int = {_ * 10}

    // TODO: 2.创建作业信息,自定义数据及处理逻辑
    val rdd = new SparkMockRDD()
    rdd.datas = datas
    //rdd.logic = logic

    // TODO: 3.切片生成分区(executor task)
    var partition_list : mutable.Queue[SparkMockTask] = rdd.splitPartition()
    if(partition_list.length <= 0){
      println("Driver(客户端)生成Executor Tasks失败...")
    }
    else{
      println("Driver(客户端)生成Executor Tasks成功...")
    }

    // TODO: 4.发送数据
    var client : Socket = null

    for(num <- 0 to partition_list.length - 1){
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
      obj_os.writeObject(partition_list(num))
      obj_os.flush()
      os.close()
      obj_os.close()
      client.close()
      println("Driver(客户端)向Executor-" + (num + 1) + "发送数据成功...")
    }

    // TODO: 4.通知
    print("Driver(客户端)数据发送完毕...")
  }
}

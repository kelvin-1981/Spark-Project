package com.yykj.spark.core.mock

import java.io.ObjectOutputStream
import java.net.Socket
import scala.collection.mutable

/**
 * Driver 负责Spark计算的主导
 */
object SparkMockDriver {

  /**
   * 模拟Driver：Diriver与Executor的通讯过程
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.待计算数据
    var datas = this.textFile("mock-path")

    // TODO: 2.创建作业信息,自定义数据及处理逻辑
    // Spark RDD 计算逻辑不可改变，不可外部传递计算逻辑
    val math_rdd = new SparkMockMathRDD(datas,_ * 100)

    // TODO: 3.分布式计算
    this.collect(math_rdd)

    // TODO: 4.通知
    print("Driver(客户端)数据发送完毕...")
  }

  /**
   * 读取数据
   * @param path
   */
  def textFile(path : String) : List[Int] = {
    List(1,2,3,4,5)
  }

  /**
   * 执行分布式计算
   * @param task_list
   */
  def runJob(task_list: Array[SparkMockTask]) : Unit = {
    // TODO: 2.发送数据
    var client : Socket = null

    for(num <- 0 to task_list.length - 1){
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
      obj_os.writeObject(task_list(num))
      obj_os.flush()
      os.close()
      obj_os.close()
      client.close()
      println("Driver(客户端)向Executor-" + (num + 1) + "发送数据成功...")
    }
  }

  /**
   * 计算
   * @return
   */
  def collect(math_rdd : SparkMockMathRDD): Unit ={

    // TODO: 1.获取分区
    val partitions = math_rdd.getPartitions()
    println("Driver(客户端)完成RDD分区..." + partitions.length)

    // TODO: 2.根据分区信息生成task集合
    var task_list = new Array[SparkMockTask](partitions.length)
    for(i <- 0 to partitions.length - 1){
      var task = new SparkMockTask
      task.datas = partitions(i).parition_data.toList
      task.logic = math_rdd.logic
      task_list(i) = task
    }

    // TODO: 3.执行计算
    runJob(task_list)
  }
}

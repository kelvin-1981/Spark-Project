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

    // TODO: 1.准备环境
    val sc = new SparkMockContext

    // TODO: 1.待计算数据
    //var f: (Int) => Int = (x: Int) => {x * 2}
    var f: (Int) => Int = {_ * 2}
    val rdd_values: SparkMockRDD[Int] = sc.parallelize[Int](List(1, 2, 3, 4, 5),2,f)

    // TODO: 3.分布式计算
    //this.collect(rdd_values)

    // TODO: 4.通知
    print("Driver(客户端)数据发送完毕...")
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
  def collect(math_rdd : SparkMockParallelRDD[Int]): Unit ={

    // TODO: 1.生成task列表
    //val taskes = math_rdd.getTaskes
    // TODO: 2.执行计算
    //runJob(taskes)
  }
}

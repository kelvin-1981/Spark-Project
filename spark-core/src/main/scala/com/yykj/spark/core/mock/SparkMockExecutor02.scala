package com.yykj.spark.core.mock

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

/**
 * Executor负责Spark计算 多个并行计算
 */
object SparkMockExecutor02 {

  /**
   * 模拟Executor：Diriver与Executor的通讯过程 第二个Executor
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建服务器
    val server = new ServerSocket(8888)
    println("Executor-2启动，等待接受数据...")

    // TODO: 2.等待客户端链接
    val client = server.accept()

    // TODO: 3.接受作业信息，执行业务操作
    val is = client.getInputStream
    val obj_is = new ObjectInputStream(is)
    var task = obj_is.readObject().asInstanceOf[SparkMockTask]
    is.close()
    obj_is.close()
    client.close()
    server.close()
    println("Executor-2接收数据:" + task.datas)

    // TODO: 4.执行业务逻辑，输出结果
    println("Executor-2开始执行Task...")
    val res_list = task.compute()
    println("Executor-2计算结果：" + res_list)
  }
}

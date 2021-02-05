package com.yykj.spark.core.mock

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

object SparkMockExecutor {

  /**
   * 模拟Executor：Diriver与Executor的通讯过程
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建服务器
    val server = new ServerSocket(9999)
    println("Executor-1(服务器)启动，等待接受数据...")

    // TODO: 2.等待客户端链接
    val client: Socket = server.accept()

    // TODO: 3.接受作业信息，执行业务操作
    val is = client.getInputStream
    val obj_is = new ObjectInputStream(is)
    val task = obj_is.readObject().asInstanceOf[SparkMockTask]
    is.close()
    obj_is.close()
    client.close()
    server.close()
    println("Executor-1(服务器)接收数据:" + task.datas)

    // TODO: 4.执行业务逻辑，输出结果
    println("Executor-1开始执行Task...")
    val res_list = task.compute()
    println("Executor-1计算结果：" + res_list)
  }
}

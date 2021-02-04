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
    println("Executor(服务器)启动，等待接受数据...")

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
    println("Executor(服务器)接收1项数据...")

    // TODO: 4.执行业务逻辑，输出结果
    println("Executor(服务器)开始执行Task...")
    val comp_res = task.compute()
    println("Executor(服务器)开始Task计算完毕...")
    println("Executor(服务器)计算结果：" + comp_res)
  }
}

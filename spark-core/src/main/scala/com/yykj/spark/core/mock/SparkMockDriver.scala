package com.yykj.spark.core.mock

import java.io.ObjectOutputStream
import java.net.Socket

object SparkMockDriver {

  /**
   * 模拟Driver：Diriver与Executor的通讯过程
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.连接服务器
    val client = new Socket("localhost", 9999)
    // TODO: 2.创建作业信息,自定义数据及处理逻辑
    val task = new SparkMockTask()
    task.datas = List(1,2,3,4,5)
    task.logic = {_ * 3}
    // TODO: 3.发送数据
    val os = client.getOutputStream
    val obj_os = new ObjectOutputStream(os)
    obj_os.writeObject(task)
    obj_os.flush()
    os.close()
    obj_os.close()
    client.close()

    // TODO: 4.通知
    print("Driver(客户端)数据发送完毕...")

  }
}

package com.yykj.spark.streaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreamingSelfReceiver {

  /**
   * 自定义数据采集器
   * 需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO: 2.业务处理
    // TODO: 2.1 使用自定义采集器 & 打印
    val myReceiver: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver)
    myReceiver.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 自定义数据采集器
   * 1.继承Receiver A.设置泛型：接入的数据类型 B.设置存储级别：StorageLevel
   * 2.重写方法
   */
  class CustomerReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var flag: Boolean = true

    /**
     * 启动时操作
     */
    override def onStart(): Unit = {
      // TODO: 1.生成采集器的线程并启动 一直处于采集状态
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            //生成数据
            val value = "data is : " + new Random().nextInt(10).toString
            //进行数据封装
            store(value)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    /**
     * 关闭采集器
     */
    override def onStop(): Unit = {
      flag = false
    }
  }

}

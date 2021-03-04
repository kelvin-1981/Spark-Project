package com.yykj.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingStateReduceByKeyAndWindow {

  /**
   * Window Operations(有状态操作):可以设置窗口的大小和滑动窗口的间隔来动态的获取当前Steaming的允许状态。
   * 所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长。
   * 1. 窗口时长：计算内容的时间范围；
   * 2. 滑动步长：隔多久触发一次计算。
   * 注意：这两者都必须为采集周期大小的整数倍。
   *
   * 注意事项:
   * 1.将多个采集周期的数据进行合并的范围称之为窗口 例如：将3个采集周期作为整体进行处理
   * 2.窗口可以滑动 数据根据滑动范围进行处理 详见WindowOperations窗口滑动.png：随着窗口滑动hello的汇总个数的变化
   *
   * reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]): 详见示例
   * 这个函数是上述函数的变化版本，每个窗口的reduce值都是通过用前一个窗的reduce值来递增计算。
   * 通过reduce进入到滑动窗口数据并”反向reduce”离开窗口的旧数据来实现这个操作。
   * 一个例子是随着窗口滑动对keys的“加”“减”计数。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    // TODO: 需要设置checkpoint
    ssc.checkpoint("cp")

    // TODO: 2.业务处理
    // TODO: 2.1 无状态化操作：只针对统计周期内数据进行计算
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val mapStream: DStream[(String, Int)] = dataStream.map((_, 1))
    // 参数1： 窗口内新增的数据
    // 参数2： 窗口内减少的数据
    val winStream: DStream[(String, Int)] = mapStream.reduceByKeyAndWindow(
      (x: Int, y: Int) => {
        x + y
      },
      (x: Int, y: Int) => {
        x - y
      },
      Seconds(9),
      Seconds(3)
    )

    winStream.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }
}

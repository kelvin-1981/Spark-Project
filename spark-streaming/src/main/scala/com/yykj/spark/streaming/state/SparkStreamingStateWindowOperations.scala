package com.yykj.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingStateWindowOperations {

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
   * 其它:关于Window的操作还有如下方法
   * （1）window(windowLength, slideInterval): 基于对源DStream窗化的批次进行计算返回一个新的Dstream；
   * （2）countByWindow(windowLength, slideInterval): 返回一个滑动窗口计数流中的元素个数；
   * （3）reduceByWindow(func, windowLength, slideInterval): 通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流；
   * （4）reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks]): 当在一个(K,V)对的DStream上调用此函数，
   *     会返回一个新(K,V)对的DStream，此处通过对滑动窗口中批次数据使用reduce函数来整合每个key的value值。
   * （5）reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]): 详见示例
   *     这个函数是上述函数的变化版本，每个窗口的reduce值都是通过用前一个窗的reduce值来递增计算。
   *     通过reduce进入到滑动窗口数据并”反向reduce”离开窗口的旧数据来实现这个操作。
   *     一个例子是随着窗口滑动对keys的“加”“减”计数。
   *     通过前边介绍可以想到，这个函数只适用于可逆的reduce函数”，也就是这些reduce函数有相应的”反reduce”函数(以参数invFunc形式传入)。
   *     如前述函数，reduce任务的数量通过可选参数来配置。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO: 2.业务处理
    // TODO: 2.1 无状态化操作：只针对统计周期内数据进行计算
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val mapStream: DStream[(String, Int)] = dataStream.map((_, 1))
    // TODO: 设置窗口范围,窗口可以滑动，默认为一个采集周期进行滑动(第一个参数:窗口时长windowDuration)
    // TODO: 重复数据情况解决 可以增加步长(第二个参数:滑动步长:slideDuration)
    val winStream: DStream[(String, Int)] = mapStream.window(Seconds(6),Seconds(6))
    val redStream: DStream[(String, Int)] = winStream.reduceByKey(_ + _)

    redStream.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }
}

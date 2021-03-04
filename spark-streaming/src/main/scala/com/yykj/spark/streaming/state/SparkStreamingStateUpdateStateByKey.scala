package com.yykj.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingStateUpdateStateByKey {

  /**
   * spark-streaming: 分为无状态化操作与有状态化操作
   * 1.无状态转化操作就是把简单的RDD转化操作应用到每个批次上，也就是转化DStream中的每一个RDD。
   * 注意，针对键值对的DStream转化操作(比如 reduceByKey())要添加import StreamingContext._才能在Scala中使用。
   * 如: map、flatmap、reduceByKey、filter、groupByKey等
   * 2.有状态化操作，记录前面采集周期的结果数据 与当前数据合并计算等 如：updateStateByKey、tranform
   * 3.执行步骤
   * 0) netcat环境准备: https://blog.csdn.net/qq_37585545/article/details/82250984
   * 1) 启动spark-streaming 程序
   * 2) 启动windows-cmd: nc -lp 9999
   * 3) 输入数据
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    //updateStateByKey需要设置checkpoint
    ssc.checkpoint("cp")

    // TODO: 2.业务处理
    // TODO: 2.1 无状态化操作：只针对统计周期内数据进行计算
    val dataStream = ssc.socketTextStream("localhost", 9999)
    val flatStream = dataStream.flatMap(_.split(" "))
    val mapStream = flatStream.map((_, 1))

    // TODO: updateStateByKey：根据key对数据状态进行更新
    // seq: 相同key的value数据集合
    // buff: 缓冲区相同key的value数据
    val stateStream: DStream[(String, Int)] = mapStream.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
      var count = buff.getOrElse(0) + seq.sum
      Option(count)
    })

    stateStream.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }
}

package com.yykj.spark.streaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 一、数据处理方式分类
 * 1.流式（Streaming）数据处理
 * 2.批量（batch）数据处理
 *
 * 二、数据处理延迟方式分类：
 * 1.实时数据处理：毫秒级
 * 2.离线数据处理：小时 OR 天
 *
 * 三、Spark-Streaming 准实时(秒 or 分钟)，微批次(秒)数据处理框架。Spark Streaming用于流式数据的处理。
 * Spark Streaming支持的数据输入源很多，例如： Kafka、Flume、 Twitter、 ZeroMQ和简单的 TCP套接字等等。
 * 数据输入后可以用 Spark的高度抽象原语如： map、 reduce、 join、 window等进行运算。
 * 而结果也能保存在很多地方，如 HDFS，数据库等。
 *
 * 四、简单来将， DStream就是对 RDD在实时数据处理场景的一种封装。
 * 和Spark基于 RDD的概念很相似， Spark Streaming使用离散化流 (discretized stream)作为抽
 * 象表示，叫作 DStream。 DStream 是随时间推移而收到的数据的序列。在内部，每个 时间区间 收
 * 到的数据都作 为 RDD 存在，而 DStream是由这些 RDD所组成的序列 (因此得名 “离散化 ”)。
 */
object SparkStreamingReceiverSocket {

  /**
   * 需求：使用 netcat工具向 9999端口不断的发送数据
   * 通过 SparkStreaming读取端口数据并统计不同单词出现的次数
   * 1. 添加依赖 pom.xml
   * 2. 编写spark-streaming 代码
   * 3. 启动程序并通过 netcat发送数据(*先启动程序后启动netcat)
   * 0) netcat环境准备: https://blog.csdn.net/qq_37585545/article/details/82250984
   * 1) 启动spark-streaming 程序
   * 2) 启动windows-cmd: nc -lp 9999
   * 3) hello spark
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // TODO: 2.业务处理
    // TODO: 2.1 获取端口数据
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // TODO: 2.2 数据计算
    val flatStream: DStream[String] = dataStream.flatMap(_.split(" "))
    val mapStream = flatStream.map((_, 1))
    val redStream: DStream[(String, Int)] = mapStream.reduceByKey(_ + _)
    // TODO: 2.3 输出打印
    redStream.print()

    // TODO: 3.环境
    // TODO: 注意：由于spark-streaming 采集器是长期执行的任务 故不能关闭
    // TODO: 注意：如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    // TODO: 3.1 启动采集器
    ssc.start()
    // TODO: 3.2 等待采集器的关闭
    ssc.awaitTermination()
  }
}

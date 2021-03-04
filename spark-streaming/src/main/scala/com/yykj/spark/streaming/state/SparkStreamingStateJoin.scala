package com.yykj.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingStateJoin {

  /**
   * Join(有状态操作):两个流之间的join需要两个流的批次大小一致，这样才能做到同时触发计算。
   * 计算过程就是对当前批次的两个流中各自的 RDD进行 join，与两个 RDD的 join效果相同。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))

    // TODO: 2.业务处理
    // TODO: 2.1 无状态化操作：只针对统计周期内数据进行计算
    val dataStream9: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val dataStream8: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    val mapStream9: DStream[(String, Int)] = dataStream9.map((_, 9999))
    val mapStream8: DStream[(String, Int)] = dataStream9.map((_, 8888))

    // TODO: 底层实际为RDD Join算子操作
    val joinStream: DStream[(String, (Int, Int))] = mapStream9.join(mapStream8)

    joinStream.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }
}

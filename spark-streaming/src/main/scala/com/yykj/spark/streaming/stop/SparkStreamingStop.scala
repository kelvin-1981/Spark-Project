package com.yykj.spark.streaming.stop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * 优雅地关闭:计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
 * 流式任务需要7*24小时执行，但是有时涉及到 升级代码 需要 主动停止程序 ，但是分布式程序，没办法做到一个个进程去杀死，所有配置优雅的关闭就显得至关重要了。
 * 使用外部文件系统来控制内部程序关闭。
 */
object SparkStreamingStop {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))

    // TODO: 2.输出操作：如果StreamingContext中没有设定输出操作，整个context就都不会启动。
    val dataStream = ssc.socketTextStream("localhost", 9999)
    val mapStream = dataStream.map((_,1))
    mapStream.print()

    // TODO: 3.环境
    ssc.start()

    // TODO: 3.1 如果想要关闭采集器，那么需要创建新的线程
    //  注释：而且需要在第三方程序中增加关闭状态
    //  1) Mysql : Table(stopSpark) => Row => data
    //  2) Redis : Data（K-V）
    //  3) ZK    : /stopSpark
    //  4) HDFS  : /stopSpark
    new Thread(new Runnable {
        override def run(): Unit = {
//          while ( true ) {
//              if (true) {
//                  // 获取SparkStreaming状态
//                  val state: StreamingContextState = ssc.getState()
//                  if ( state == StreamingContextState.ACTIVE ) {
//                      ssc.stop(true, true)
//                      System.exit(0)
//                  }
//              }
//              Thread.sleep(5000)
//          }

          Thread.sleep(5000)
          val state: StreamingContextState = ssc.getState()
          if ( state == StreamingContextState.ACTIVE ) {
            // TODO: stopGracefully= true 优雅的关闭
            ssc.stop(true, true)
          }
          System.exit(0)
        }}
    ).start()

    ssc.awaitTermination() // block 阻塞main线程
  }

}

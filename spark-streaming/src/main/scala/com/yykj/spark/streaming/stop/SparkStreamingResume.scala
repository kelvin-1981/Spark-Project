package com.yykj.spark.streaming.stop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * 优雅地关闭:关闭后恢复数据
 */
object SparkStreamingResume {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.关闭后数据恢复 从检查点恢复数据 如无法恢复执行代码段
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
      val ssc = new StreamingContext(conf, Seconds(5))

      // TODO: 2.输出操作：如果StreamingContext中没有设定输出操作，整个context就都不会启动。
      val dataStream = ssc.socketTextStream("localhost", 9999)
      val mapStream = dataStream.map((_, 1))
      mapStream.print()

      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()
    ssc.awaitTermination() // block 阻塞main线程
  }

}

package com.yykj.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreamingReceiverQueue {

  /**
   * 测试过程中，可以通过使用 ssc.queueStream(queueOfRDDs)来创建 DStream，每一个推送到这个队列中的 RDD，都会作为一个 DStream处理。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO: 2.业务处理
    // TODO: 2.1 声明Queue队列作为输入源 进行数据计算
    val rddQue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    val inStream: InputDStream[Int] = ssc.queueStream(rddQue, false)
    val mapStream: DStream[(Int, Int)] = inStream.map((_, 1))
    val redStream: DStream[(Int, Int)] = mapStream.reduceByKey(_ + _)
    redStream.print()

    // TODO: 3.环境
    // TODO: 注意：由于spark-streaming 采集器是长期执行的任务 故不能关闭
    // TODO: 注意：如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    // TODO: 3.1 启动采集器 
    ssc.start()

    // TODO: 4.创建模拟数据
    for (i <- -1 to 5){
      rddQue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    // TODO: 3.2 等待采集器的关闭(用户的操作作为关闭信号)
    ssc.awaitTermination()
  }
}

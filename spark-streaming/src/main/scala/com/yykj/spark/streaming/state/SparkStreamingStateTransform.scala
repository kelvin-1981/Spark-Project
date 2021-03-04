package com.yykj.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingStateTransform {

  /**
   * Transform(有状态操作):允许DStream上执行任意的RDD-to-RDD函数。
   * 即使这些函数并没有在DStream的API中暴露出来，通过该函数可以方便的扩展Spark API。
   * 该函数每一批次调度一次。其实也就是对DStream中的RDD应用转换。
   * 应用场景： 1.DStream功能不丰富
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO: 2.业务处理
    // TODO: 2.1 无状态化操作：只针对统计周期内数据进行计算
    val dataStream = ssc.socketTextStream("localhost", 9999)

    val resRDD: DStream[(String, Int)] = dataStream.transform(rdd => {
      val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
      val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
      val redRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
      redRDD
    })

    resRDD.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }
}

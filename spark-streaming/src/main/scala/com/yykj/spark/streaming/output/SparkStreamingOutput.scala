package com.yykj.spark.streaming.output

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 输出操作指定了对流数据经转化操作得到的数据所要执行的操作(例如把结果推入外部数据库或输出到屏幕上)。
 * 与RDD中的惰性求值类似，如果一个DStream及其派生出的DStream都没有被执行输出操作，那么这些DStream就都不会被求值。
 * 如果StreamingContext中没有设定输出操作，整个context就都不会启动。
 * 输出操作：print、saveAsTextFiles、saveAsObjectFiles、saveAsHadoopFiles、foreachRDD等
 */
object SparkStreamingOutput {

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
    // TODO: 2.1 print：在运行流程序的驱动结点上打印DStream中每一批次数据的最开始10个元素。这用于开发和调试。在Python API中，同样的操作叫print()。
    //streamOutputPrint(ssc)
    // TODO: 2.2 saveAsTextFiles：以text文件形式存储这个DStream的内容。每一批次的存储文件名基于参数中的prefix和suffix。
    //streamOutputSaveAsTextFiles(ssc)
    // TODO: 2.3 foreachRDD：这是最通用的输出操作，即将函数 func 用于产生于 stream的每一个RDD。
    //  其中参数传入的函数func应该实现将每一个RDD中数据推送到外部系统，如将RDD存入文件或者通过网络将其写入数据库。
    streamOutputForeachRDD(ssc)

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * print:打印操作
   * @param ssc
   */
  def streamOutputPrint(ssc: StreamingContext): Unit = {
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    dataStream.print()
  }

  /**
   * saveAsTextFiles操作
   * @param ssc
   */
  def streamOutputSaveAsTextFiles(ssc: StreamingContext): Unit = {
    // TODO: 控制台不会出现时间戳信息
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    dataStream.saveAsObjectFiles("spark-streaming/datas/output")
  }

  /**
   * foreachRDD操作
   * @param ssc
   */
  def streamOutputForeachRDD(ssc: StreamingContext): Unit = {
    // TODO: 控制台不会出现时间戳信息
    val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    dataStream.foreachRDD(rdd => {
      rdd.collect().foreach(println)
    })
  }
}

package com.yykj.spark.streaming.samples

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 1.需求说明：最近一小时广告点击量 总数
 */
object SparkStreamingHourAnalysis02 {

  def main(args: Array[String]): Unit = {

    // TODO: 1.声明环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("SS")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("cp")

    // TODO: 2.配置kafka
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node21:9092, node22:9092, node23:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "yykj",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // TODO: 3.读取kafka数据
    val inStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("yykj"), kafkaPara)
    )

    // TODO: 4. 统计一小时点击总数
    val cntStream: DStream[Long] = inStream.countByWindow(Seconds(30), Seconds(3))

    // TODO: 5. 打印
    cntStream.print()

    // TODO: 6. 数据采集器启动
    ssc.start()
    ssc.awaitTermination()
  }
}

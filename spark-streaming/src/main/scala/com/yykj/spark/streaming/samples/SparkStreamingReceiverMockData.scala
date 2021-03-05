package com.yykj.spark.streaming.samples

import java.util.{Properties, Random}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreamingReceiverMockData {

  /**
   * 模拟数据生成 & 发送kafka & 接收 & 打印数据
   * 一、需求过程：application-》 kafka-》 spark-streaming
   * 二、执行步骤
   * 1) 【全部节点】启动zookeeper: /opt/module/zookeeper-3.4.14/bin/zkServer.sh start
   * 2) 【全部节点】启动kafka: /opt/module/kafka-2.1.1/bin/kafka-server-start.sh /opt/module/kafka-2.1.1/config/server.properties &
   * 3) kafka集群创建topic:
   *   A./opt/module/kafka-2.1.1/bin/kafka-topics.sh --list --zookeeper node21:2181
   *   B./opt/module/kafka-2.1.1/bin/kafka-topics.sh --create --zookeeper node21:2181 --partitions 3 --replication-factor 2 --topic yykj
   * 4) 启动spark-streaming mock data程序
   * 5) 启动spark-streaming receiver程序(此程序)
   * 6) 执行结果：mock 发送数据至kafka receiver 接收数据并打印
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.环境配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("REQ-01")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // TODO: 2.kafka环境配置
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node21:9092, node22:9092, node23:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "yykj",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // TODO: 3.接收数据
    val inStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("yykj"), kafkaPara)
    )
    // TODO: 3.输出接收数据
    inStream.map(_.value()).print()

    // TODO: 4.数据采集器启动
    ssc.start()
    ssc.awaitTermination()
  }
}

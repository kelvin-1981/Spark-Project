package com.yykj.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreamingKafkaReceiver {

  /**
   * kafka数据采集器 DirectAPI：是由计算的Executor来主动消费Kafka的数据，速度由自身控制。
   * 1) 【全部节点】启动zookeeper: /opt/module/zookeeper-3.4.14/bin/zkServer.sh start
   * 2) 【全部节点】启动kafka: /opt/module/kafka-2.1.1/bin/kafka-server-start.sh /opt/module/kafka-2.1.1/config/server.properties &
   * 3) kafka集群创建topic:
   *    A./opt/module/kafka-2.1.1/bin/kafka-topics.sh --list --zookeeper node21:2181
   *    B./opt/module/kafka-2.1.1/bin/kafka-topics.sh --create --zookeeper node21:2181 --partitions 3 --replication-factor 2 --topic yykj
   * 4) 启动spark-streaming程序
   * 5) producer发送数据至topic yykj: /opt/module/kafka-2.1.1/bin/kafka-console-producer.sh --broker-list node21:9092 --topic yykj
   * 6) 输出计算结果完成
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境对象
    // Duration:采集周期 默认单位毫秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO: 2.业务处理
    // TODO: 2.1 kafka配置
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node21:9092, node22:9092, node23:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "yykj",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // TODO: 2.2 读取kafka数据
    // 1.KafkaUtils.createDirectStream 创建kafka数据采集器
    // 2.LocationStrategies.PreferConsistent: 数据采集节点与计算节点如何匹配：框架自动匹配
    // 3.ConsumerStrategies.Subscribe: 消费者策略 yykj：topic kafkaPara：配置
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("yykj"), kafkaPara)
    )

    // TODO: 2.3 获取kafka消息的数据 & 计算
    kafkaStream.map(_.value()).print()

//    val mapStream: DStream[(String, Int)] = kafkaStream.map {
//      case record => (record.value(), 1)
//    }
//    val redStream: DStream[(String, Int)] = mapStream.reduceByKey(_ + _)
//    redStream.print()

    // TODO: 3.环境
    ssc.start()
    ssc.awaitTermination()
  }
}

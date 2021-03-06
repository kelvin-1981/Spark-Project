package com.yykj.spark.streaming.samples

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * 1.需求说明：最近一小时广告点击量 趋势 （最近一分钟，每十秒统计一次）
 * 2.需求过程：mock data -> kafka-> spark-streaming -> analysis -> json -> html
 * 3.需求结果：
 *   1) List [15:50 -->10,15:51 -->25,15:52 -->30]
 *   2) List [15:50 -->10,15:51 -->25,15:52 -->30]
 *   3) List [15:50 -->10,15:51 -->25,15:52 -->30]
 * 4.数据定义
 *   1) 格式 ：timestamp area city userid adid
 *   2) 含义： 时间戳 区域 城市 用户 广告
 * 5.执行步骤
 *   1) 【全部节点】启动zookeeper: /opt/module/zookeeper-3.4.14/bin/zkServer.sh start
 *   2) 【全部节点】启动kafka: /opt/module/kafka-2.1.1/bin/kafka-server-start.sh /opt/module/kafka-2.1.1/config/server.properties &
 *   3) kafka集群创建topic:
 *      A./opt/module/kafka-2.1.1/bin/kafka-topics.sh --list --zookeeper node21:2181
 *      B./opt/module/kafka-2.1.1/bin/kafka-topics.sh --create --zookeeper node21:2181 --partitions 3 --replication-factor 2 --topic yykj
 *   4) 导入datas/adclick 含html、json
 *   5) 启动spark-streaming SparkStreamingHourAnalysis
 *   6) 启动spark-streaming SparkStreamingMockData
 *   7) 计算完成
 */
object SparkStreamingHourAnalysis {

  def main(args: Array[String]): Unit = {

    // TODO: 1.声明环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("SS")
    val ssc = new StreamingContext(conf, Seconds(5))
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

    // TODO: 4.进行周期性（窗口）数据计算：最近一分钟，每十秒统计一次
    val redStream: DStream[(Long, Int)] = inStream.map(record => {
      val dataArr: Array[String] = record.value().split(" ")
      val ts: Long = dataArr(0).toLong / 10000 * 10000
      (ts, 1)
    }).reduceByKeyAndWindow((x1: Int, x2: Int) => {x1 + x2}, Seconds(60), Seconds(10))

    // TODO: 5.输出至json文件 前端定时读取json文件进行图形展现
    redStream.foreachRDD(rdd => {
      val list = ListBuffer[String]()
      val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
      datas.foreach{
        case (ts, sum) => {
          val time = new SimpleDateFormat("mm:ss").format(new java.util.Date(ts.toLong))
          list.append(s"""{"xtime":"${time}", "yval":"${sum}"}""")
        }
      }
      // 输出文件
      val out = new PrintWriter(new FileWriter(new File("spark-streaming/datas/adclick/adclick.json")))
      out.println("["+list.mkString(",")+"]")
      out.flush()
      out.close()
    })

    // TODO: 6. 数据采集器启动
    ssc.start()
    ssc.awaitTermination()
  }
}

package com.yykj.spark.streaming.samples

import java.text.SimpleDateFormat
import java.util.Date

import com.yykj.spark.streaming.samples.utils.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SPARK_BRANCH, SPARK_BUILD_DATE, SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamAdAnalysis {

  /**
   * 实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL。
   * 1.需求过程：mock data -> kafka-> spark-streaming -> analysis -> mysql
   * 2.数据定义
   *   1) 格式 ：timestamp area city userid adid
   *   2) 含义： 时间戳 区域 城市 用户 广告
   * 3.执行步骤
   *   1) 【全部节点】启动zookeeper: /opt/module/zookeeper-3.4.14/bin/zkServer.sh start
   *   2) 【全部节点】启动kafka: /opt/module/kafka-2.1.1/bin/kafka-server-start.sh /opt/module/kafka-2.1.1/config/server.properties &
   *   3) kafka集群创建topic:
   *   A./opt/module/kafka-2.1.1/bin/kafka-topics.sh --list --zookeeper node21:2181
   *   B./opt/module/kafka-2.1.1/bin/kafka-topics.sh --create --zookeeper node21:2181 --partitions 3 --replication-factor 2 --topic yykj
   *   4) 创建MySQL数据表 area_city_ad_count
   *   4) 启动spark-streaming SparkStreamAdAnalysis程序
   *   5) 启动spark-streaming SparkStreamingMockData程序
   *   6) 计算完成
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.声明环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("AD")
    val ssc = new StreamingContext(conf, Seconds(3))

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
      ConsumerStrategies.Subscribe[String, String](Seq[String]("yykj"), kafkaPara)
    )

    // TODO: 4.业务逻辑
    // TODO: 4.1 转换数据结构
    val mapStream: DStream[adAction] = inStream.map(record => {
      val dataArr: Array[String] = record.value().split(" ")
      adAction(dataArr(0), dataArr(1), dataArr(2), dataArr(3), dataArr(4))
    })

    // TODO: 4.2 计算周期内的数据汇总计算
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val tranStream: DStream[((String, String, String, String), Int)] = mapStream.transform(rdd => {
      val redRDD: RDD[((String, String, String, String), Int)] = rdd.map(info => {
        //((day, area, city, ad), 1)
        ((sdf.format(new Date(info.day.toLong)), info.area, info.city, info.ad), 1)
      }).reduceByKey(_ + _)

      redRDD
    })

    // TODO: 4.3 根据周期数据结算结果 更新至MySQL数据库
    tranStream.foreachRDD(rdd => {
      rdd.foreach{
        case ((day, area, city, ad), sum) => {
          println(s"${day} ${area} ${city} ${ad} ${sum}")
          insertOrElseAreaAction(day, area, city, ad, sum)
        }
      }
    })

    // TODO: 4.4 数据采集器启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 根据主键状态插入、更新数据
   * @param
   */
  def insertOrElseAreaAction(day: String,area: String, city: String, ad: String, sum: Int): Unit = {
    val conn = JDBCUtil.getConnection
    JDBCUtil.executeUpdate(
      conn,
      "insert into area_city_ad_count values (?,?,?,?,?) ON DUPLICATE KEY UPDATE count=count + ?",
      Array(day, area, city, ad, sum, sum)
    )
    conn.close()
  }

  /**
   * 时间戳 区域 城市 用户 广告
   */
  case class adAction(var day: String, var area: String, var city: String, var user: String, var ad: String)
}

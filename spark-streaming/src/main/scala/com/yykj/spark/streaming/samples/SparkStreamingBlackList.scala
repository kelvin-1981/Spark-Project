package com.yykj.spark.streaming.samples

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.yykj.spark.streaming.samples.utils.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。注：黑名单保存到MySQL中。
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
 *   4) 创建MySQL数据表 black_list、user_ad_count
 *   4) 启动spark-streaming SparkStreamingBlackList程序
 *   5) 启动spark-streaming SparkStreamingMockData程序
 *   6) 计算完成
 */
object SparkStreamingBlackList {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.声明环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("SQ")
    val ssc = new StreamingContext(conf, Seconds(5))

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

    // TODO: 4.黑名单业务逻辑
    // TODO: 4.1 数据解析 & 转换
    val mapStream: DStream[adAction] = inStream.map(record => {
      //数据定义: timestamp area city userid adid
      val dataArr: Array[String] = record.value().split(" ")
      adAction(dataArr(0), dataArr(1), dataArr(2), dataArr(3), dataArr(4))
    })


    // TODO: 4.2 周期性获取黑名单信息
    //  A.过滤已经进入黑名单用户
    //  B.未在黑名单，统计当天的累计点击数，如果统计结果超过阈值更新入黑名单
    //  B.未在黑名单，统计当天的累计点击数，如果统计结果未超过阈值更新数据库点击表
    val redStream: DStream[((String, String, String), Int)] = mapStream.transform(rdd => {
      // TODO: 获取DB已经存在的黑名单
      val blackList: ListBuffer[String] = getBlackListInDB()
      // TODO: 过滤已经进入黑名单用户
      val filterRDD: RDD[adAction] = rdd.filter(action => {
        !blackList.contains(action.user)
      })

      // TODO: 逐条处理数据(未在黑名单内的用户) 转化类型 & 数据计算
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val redRDD: RDD[((String, String, String), Int)] = filterRDD.map(info => {
        //((day, user, ad), 1)
        ((sdf.format(new Date(info.day.toLong)), info.user, info.ad), 1)
      }).reduceByKey(_ + _)
      redRDD
    })


    redStream.foreachRDD(rdd => {
      rdd.foreach{
        case ((day, user, ad), sum) => {

          println(s"${day} ${user} ${ad} ${sum}")

          // TODO: A.如果统计结果超过阈值更新入黑名单 B.如果统计结果未超过阈值更新数据库点击表
          if(sum >= 30){
            insertOrElseBlackList(user)
          }
          else{
            // TODO: 获取之前保存的数据 点击数量sum
            val sumBefore: Int = getActionSum(day, user, ad)
            val sumNew: Int = sumBefore + sum
            // TODO: 如最新点击数量大于阈值 保存至黑名单 反之更新数据表
            if(sumNew >= 30){
              insertOrElseBlackList(user)
            }else{
              insertOrElseAction(day, user, ad, sum)
            }
          }
        }
      }
    })

    // TODO: 6.数据采集器启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   *
   * @return
   */
  def getBlackListInDB(): ListBuffer[String] = {
    // 周期性获取黑名单信息
    val blackList = ListBuffer[String]()
    val conn = JDBCUtil.getConnection
    val stat = conn.prepareStatement("select * from black_list")
    val rs = stat.executeQuery()
    while (rs.next()){
      blackList.append(rs.getString(1))
    }
    rs.close()
    stat.close()
    conn.close()

    blackList
  }

  /**
   * 根据主键状态插入、更新黑名单数据
   * @param user
   */
  def insertOrElseBlackList(user: String): Unit = {
    // 周期性获取黑名单信息
    val conn = JDBCUtil.getConnection
    //当出现重复主键时 只做更新数据
    val stat = conn.prepareStatement("insert into black_list values (?) ON DUPLICATE KEY UPDATE userid=?")
    stat.setString(1, user)
    stat.setString(2, user)
    stat.executeUpdate()
    stat.close()
    conn.close()
  }

  /**
   * 根据主键状态插入、更新Action数据
   * @param user
   */
  def insertOrElseAction(day: String, user: String, ad: String, sum: Int): Unit = {
    // 周期性获取黑名单信息
    val conn = JDBCUtil.getConnection

    val stat = conn.prepareStatement("insert into user_ad_count values (?,?,?,?) ON DUPLICATE KEY UPDATE count = count + ?")
    stat.setString(1, day)
    stat.setString(2, user)
    stat.setString(3, ad)
    stat.setInt(4, sum)
    stat.setInt(5, sum)
    stat.executeUpdate()
    stat.close()
    conn.close()
  }


  /**
   * 获取动作表的数据
   * @param day
   * @param user
   * @param ad
   */
  def getActionSum(day: String, user: String, ad: String): Int = {

    val conn = JDBCUtil.getConnection

    val stat = conn.prepareStatement("select * from user_ad_count where dt = ? and userid = ? and adid = ?")
    stat.setString(1, day)
    stat.setString(2, user)
    stat.setString(3, ad)

    var sum = 0
    val rs: ResultSet = stat.executeQuery()
    if(rs.next()){
      sum = rs.getInt(4)
    }
    stat.close()
    conn.close()

    sum
  }

  /**
   * 时间戳 区域 城市 用户 广告
   */
  case class adAction(var day: String, var area: String, var city: String, var user: String, var ad: String)
}

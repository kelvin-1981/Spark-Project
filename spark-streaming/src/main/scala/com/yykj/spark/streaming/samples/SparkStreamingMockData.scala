package com.yykj.spark.streaming.samples

import java.util.{Properties, Random}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.collection.mutable.ListBuffer

object SparkStreamingMockData {

  /**
   * 模拟生成流式数据
   * 1.需求过程：application-》 kafka-》 spark-streaming
   * 2.数据定义
   *   1) 格式 ：timestamp area city userid adid
   *   2) 含义： 时间戳 区域 城市 用户 广告
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.kafka参数配置
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node21:9092, node22:9092, node23:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    
    // TODO: 2.生成kafka producer 
    val producer = new KafkaProducer[String, String](prop)

    // TODO: 3.生成模拟数据 & 发送至kafka
    while(true){
      val dataList: ListBuffer[String] = mockData()
      dataList.foreach(data => {
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("yykj", data)
        producer.send(record)
        println(data)
      })

      Thread.sleep(3000)
    }
  }

  /**
   * 生成模拟数据
   * 1.格式 ：timestamp area city userid adid
   * 2.含义： 时间戳 区域 城市 用户 广告
   * @return
   */
  def mockData(): ListBuffer[String] = {
    val dataList: ListBuffer[String] = ListBuffer[String]()
    val areaList: ListBuffer[String] = ListBuffer[String]("华北", "华东", "东北", "西南", "东南")
    val cityList: ListBuffer[String] = ListBuffer[String]("北京", "上海", "广州", "深圳", "武汉","三亚")

    for(i <- 1 to new Random().nextInt(50)){
      val area: String = areaList(new Random().nextInt(5))
      val city: String = cityList(new Random().nextInt(6))
      val userid: String = "U" + (new Random().nextInt(6) + 1)
      var adid: String = "AD" + (new Random().nextInt(6) + 1)
      dataList.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }

    dataList
  }
}

package com.yykj.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object SparkBroadcast {
  /**
   * 广播变量用来高效分发较大的对象。
   * 向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
   * 比如，如果你的应用需要向所有节点发送一个较大的只读查询表，广播变量用起来都很顺手。
   * 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送，造成性能低下。
   * 广播变量只读，在一个executor执行多个task时共享使用
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.业务逻辑:需要实现dataRDD_1.join(dataRDD_2)效果
    // TODO: 2.1 创建数据
    val dataRDD_1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //val dataRDD_2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    val dataMap = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))
    // TODO: 2.2 创建广播变量
    val bc = sc.broadcast(dataMap)
    // TODO: 2.3 实现逻辑,使用广播变量
    val joinRDD: RDD[(String, (Int, Int))] = dataRDD_1.map {
      case (word, count) => {
        val num = bc.value.getOrElse(word, 0)
        (word, (count, num))
      }
    }
    // TODO: 2.4 输出
    joinRDD.collect().foreach(println)
    // TODO: 3.关闭环境
    sc.stop()
  }
}

package com.yykj.spark.core.samples.ec

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategorySessionAnalysis {

  /**
   * 电商案例-需求2：在需求一的基础上，增加每个品类用户session的点击统计
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.数据说明
    /**
     * 上面的数据图是从数据文件中截取的一部分内容，表示为电商网站的用户行为数据，主
     * 要包含用户的 4 种行为： 搜索，点击，下单，支付 。 数据规则如下：
     * ➢ 数据文件中每行数据采用_分隔数据
     * ➢ 每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
     * ➢ 如果搜索关键字为 null, 表示数据不是搜索数据
     * ➢ 如果点击的品类ID和产品ID为-1，表示数据不是点击数据
     * ➢ 针对于下单行为一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
     * ➢ 支付行为和下单行为类似
     */

    // TODO: 2.开发
    // TODO: 2.1 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("SC")
    val sc = new SparkContext(conf)

    // TODO: 2.2 业务逻辑
    // TODO: 2.1 获取数据
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/sample/user_visit_action.txt")
    dataRDD.cache()

    // TODO: 2.2 获取品类热度TOP10
    val top10Array: Array[String] = getTop10HotCategory(dataRDD)

    // TODO: 2.3 声明、创建广播 (此示例只读取top10Array数值不修改 && top10Array数据量不大 所以不用广播)
    //val bc: Broadcast[Array[String]] = sc.broadcast(topArray)

    // TODO: 2.4 过滤
    val filterRDD: RDD[String] = dataRDD.filter(line => {
      //val topList: List[String] = bc.value.toList
      var arr: Array[String] = line.split("_")
      var code = arr(6)
      if(top10Array.contains(code) && code != "-1"){
        true
      } else{
        false
      }
    })

    // TODO: 2.4 获取前十名类别的Session点击量TOP10 转换数据结构
    val mapRDD: RDD[((String, String), Int)] = filterRDD.map(line => {
      var arr: Array[String] = line.split("_")
      var code = arr(6)
      var session = arr(2)
      ((code, session), 1)
    })

    // TODO: 2.5 计算
    val redRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    // TODO: 2.6 转换格式
    val mapRDD_2: RDD[(String, (String, Int))] = redRDD.map {
      case ((code, session), num) => {
        (code, (session, num))
      }
    }

    // TODO: 2.7 分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD_2.groupByKey()

    // TODO: 2.8 排序 & TOP10
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.map {
      case (code, iter) => {
        val takeList: List[(String, Int)] = iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        (code, takeList)
      }
    }

    // TODO: 2.9 输出
    resRDD.collect().foreach(println)

    // TODO: 3 环境关闭
    sc.stop()
  }


  /**
   * 使用累加器实现 减少shuffle
   * @param sc
   */
  def getTop10HotCategory(dataRDD:RDD[String]): Array[String] = {
    // TODO: 2.生成数据结构
    val mapRDD: RDD[(String, (Int, Int,Int))] = dataRDD.flatMap(line => {
      var arr: Array[String] = line.split("_")
      var structMap = mutable.Map[String, (Int, Int,Int)]()
      // 点击量
      if (arr(6) != "-1") {
        structMap.update(arr(6), (1, 0, 0))
      }
      // 下单量
      if (arr(8) != "null") {
        val codes: Array[String] = arr(8).split(",")
        codes.map(structMap.update(_, (0, 1, 0)))
      }
      // 支付量
      if (arr(10) != "null") {
        val codes: Array[String] = arr(10).split(",")
        codes.map(structMap.update(_, (0, 0, 1)))
      }

      structMap
    })

    // TODO: 3.汇总计算
    val reduceRDD: RDD[(String, (Int, Int, Int))] = mapRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    // TODO: 4.排序 & TOP10
    val takeArray: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false).take(10)

    // TODO: 5.获取名称
    val cateArray: Array[String] = takeArray.map(_._1)

    cateArray
  }
}

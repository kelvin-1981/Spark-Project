package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDAggregateByKey {

  /**
   * AggregateByKey:标准操作[Transform &  Key-Value类型]
   * 将数据根据不同的规则进行分区内计算和分区间计算
   * 相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
   * 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
   * def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 AggregateByKey:将数据根据不同的规则进行分区内计算和分区间计算
    //transformAggregateByKey(sc)
    // TODO: 2.2 AggregateByKey:分区内取最大值，分区间进行最大值乘积计算
    //transformAggregateByKey02(sc)
    // TODO: 2.3 AggregateByKey 初始值的设置及作用(求平均值)
    transformAggregateByKeyZeroValue(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * AggregateByKey:将数据根据不同的规则进行分区内计算和分区间计算
   * @param sc
   */
  def  transformAggregateByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    // TODO: aggregateByKey存在参数柯里化 参数1:初始值,第一个元素与初始值进行计算 参数2:(分区内计算规则,分区间计算规则)
    // TODO: 计算步骤1(分区内计算) Partition1：(a,(a,3)) Partition2：(a,(a,7))
    // TODO: 计算步骤2(分区间计算) (a,21)
    val aggRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(_ + _, _ * _)
    aggRDD.collect().foreach(println)
  }

  /**
   * AggregateByKey:分区内取最大值，分区间进行最大值乘积计算
   * @param sc
   */
  def  transformAggregateByKey02(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    // TODO: aggregateByKey存在参数柯里化 参数1:初始值,第一个元素与初始值进行计算 参数2:(分区内计算规则,分区间计算规则)
    // TODO: 计算步骤1(分区内计算) Partition1：(a,(a,2)) Partition2：(a,(a,4))
    // TODO: 计算步骤2(分区间计算) (a,8)
    val aggRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(
      (x1, x2) => math.max(x1, x2),
      (x1, x2) => x1 * x2
    )
    aggRDD.collect().foreach(println)
  }

  /**
   * AggregateByKey 初始值的设置及作用
   * @param sc
   */
  def transformAggregateByKeyZeroValue(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4),("b", 5),("a", 6)), 2)
    // TODO: 1.AggregateByKey算子最终返回结果与ZeroValue的数据类型保持一致
    // TODO: 2.此示例求平均值 需要计算两个数值（总值及次数）使用tuple分区内结果的记录 tuple _1,_2对应指标项自定义
    val aggRDD: RDD[(String, (Int, Int))] = dataRDD.aggregateByKey((0, 0))(
      // TODO: 2.1分区内计算：tuple (数值之和，key数量)
      (t, v) => (t._1 + v, t._2 + 1),
      // TODO: 2.2分区间计算：tuple (数值之和，数量之和)
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    // TODO: 3.针对key-value的数值进行处理使用mapValues算子
    val mapVRDD: RDD[(String, Int)] = aggRDD.mapValues(tup => {
      tup._1 / tup._2
    })
    // TODO: 4.输出
    mapVRDD.collect().foreach(println)
  }
}

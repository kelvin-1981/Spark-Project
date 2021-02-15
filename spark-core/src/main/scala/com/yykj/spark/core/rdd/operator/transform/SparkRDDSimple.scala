package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDSimple {

  /**
   * simple:标准操作[Transform & Value单值类型] 使用场景：抽取数据进行分析，预防数据倾斜
   * 根据指定的规则从数据集中抽取数据
   * def sample(withReplacement: Boolean,fraction: Double,seed: Long = Utils.random.nextLong): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 sample标准操作 根据指定的规则从数据集中抽取数据
    // 第一个参数：抽取的数据是否放回， false ：不放回
    // 第二个参数：抽取的几率，范围在 [ 之间 ,0 ：全不取 1 ：全取
    // 第三个参数：随机数种子
    transformSample(sc)


    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * sample标准操作 根据指定的规则从数据集中抽取数据
   * @param sc
   */
  def transformSample(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // TODO: 算子参数解释
    // 第一个参数：抽取的数据是否放回， true:放回 false:不放回
    // 第二个参数：抽取的几率(不准确)，范围在 [ 之间 ,0 ：全不取 1 ：全取
    // 第三个参数：随机数种子

    //val simpleRDD: RDD[Int] = dataRDD.sample(false, 0.4)
    val sampleRDD: RDD[Int] = dataRDD.sample(true, 0.4)

    sampleRDD.collect().foreach(println)
  }
}

package com.yykj.spark.core.mock

import scala.collection.mutable

/**
 * Spark RDD:
 * 1.具备弹性：存储、容错、计算、分区
 * 2.分布式：数据存储在分布式大数据平台 RDD进行分布式计算；
 * 3.计算：RDD是Spark最小计算单元，可以通过RDD组合完成丰富功能
 * 4.数据模型：用于封装计算逻辑，不保存数据
 * 5.抽象类
 * 6.不可变：RDD封装了计算逻辑，不可以改变。如需新计算逻辑需要编写新的RDD
 * 7.可分区：实现并行计算
 */
class SparkMockMathRDD extends SparkMockRDD {

  /**
   * 数据 : _代表空值
   * Spark RDD 不保存数据
   */
  var datas: List[Int] = _

  /**
   * 处理逻辑:Spark RDD 计算逻辑不可改变
   * 声明logic的类型(函数)  var logic : (Int) => Int
   */
  //val logic = (num : Int) => { num * 2 }
  //var logic : (Int) => Int = {_ * 2}
  //_代表空值
  var logic: (Int) => Int = _

  /**
   *
   * @param f
   * @param data
   */
  def this(data : List[Int],f : (Int) => Int) = {
    this()
    datas = data
    logic = f
  }

  /**
   * 获取分区列表
   *
   * @return
   */
  override def getPartitions(): Array[SparkMockPartition] = {
    this.paritioner.datas_all = datas
    this.paritioner.createParitiones()
  }

  /**
   * 分布式计算方法
   *
   * @param parition
   * @return
   */
  override def compute(parition: SparkMockTask): Iterator[Any] = ???
}

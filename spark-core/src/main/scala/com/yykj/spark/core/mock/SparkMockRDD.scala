package com.yykj.spark.core.mock

import scala.reflect.{classTag, ClassTag}

/**
 * RDD基类 包含五大属性
 * 1.分区列表:getPartitions
 * 2.计算方法:compute
 * 3.RDD依赖关系:getDependencies
 * 4.分区器:paritioner
 * 5.首选位置:判断计算发送给哪个节点最有 优先发给本地有数据的计算节点
 * @tparam T
 */
abstract class SparkMockRDD[T: ClassTag] {

  /**
   * 分区器
   */
  var paritioner = new SparkMockParitioner

  /**
   * 获取分区列表
   * @return
   */
  def getPartitions() : Array[SparkMockPartition]

  /**
   * 分布式计算方法
   * @param parition
   * @return
   */
  def compute(parition: SparkMockTask) : Iterator[Any]

}

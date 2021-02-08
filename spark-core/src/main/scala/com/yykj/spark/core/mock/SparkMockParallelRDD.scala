package com.yykj.spark.core.mock

import org.apache.spark.Partition

import scala.reflect.ClassTag

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
class SparkMockParallelRDD[T: ClassTag](sc: SparkMockContext,
                                        data : Seq[T],
                                        numSlices: Int,
                                        logic : (Int) => Int) extends SparkMockRDD[T](sc){

  /**
   * 1.[Spark一致]获取分区列表
   *
   * @return
   */
  override def getPartitions(): Array[SparkMockPartition] = {
    // TODO: 分配分区数据
    val slices = SparkMockParallelRDD.slice(data, numSlices)
    // TODO: 创建分区集合
    // indices获取数组的下标 Array(100,200,300) : (0,1,2)
    slices.indices.map(i => new SparkMockParallelPartition(1,i,slices(i))).toArray
  }

  /**
   * 2.分布式计算方法
   *
   * @param split
   * @return
   */
  override def compute(split: SparkMockPartition): Iterator[Nothing] = ???

  /**
   * 3.[Spark一致]依赖关系列表（本示例未使用）
   *
   * @return
   */
  override protected def getDependencies: Seq[Int] = ???

  /**
   * 4.[Spark一致]获取最佳计算位置
   *
   * @param split
   * @return
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = ???

}

object SparkMockParallelRDD {
  /**
   * 分区分配数据算法
   * @param seq
   * @param numSlices
   * @tparam T
   * @return
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("分区数量<=0")
    }

    def positions(length: Long,numSlices: Int):Iterator[(Int,Int)] = {
      (0 until numSlices).iterator.map { i => {
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }}
    }

    var array = seq.toArray
    positions(array.length,numSlices).map{ case(start,end) => {
      array.slice(start,end).toSeq
    }}.toSeq
  }
}

package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCoalesce {

  /**
   * coalesce:标准操作[Transform & Value单值类型] : 进行分区数量的重新设置(缩小或扩大分区数量)
   * 通常场景：根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
   * 当spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，减少分区的个数，减小任务调度成本
   * def coalesce(numPartitions: Int, shuffle: Boolean = false,partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
   * (implicit ord: Ordering[T] = null): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 coalesce:根据数据情况实现小数据集分区的合并(减少分区)，提升效率
    //transformCoalesce(sc)
    // TODO: 2.2 减少分区 & shuffle数据
    //transformCoalesce02(sc)
    // TODO: 2.3 增加分区 & shuffle数据
    transformCoalesce03(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * coalesce:根据数据情况实现小数据集分区的合并(减少分区)，提升效率
   * @param sc
   */
  def transformCoalesce(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)
    // TODO: 将原有的4个分区缩减为2个
    val coalRDD: RDD[Int] = dataRDD.coalesce(2)
    coalRDD.saveAsTextFile("spark-core/datas/output/")
  }

  /**
   * coalesce:减少分区 & shuffle数据
   * @param sc
   */
  def transformCoalesce02(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)
    // TODO: 1.coalesce默认不会重新shuffle 分区数量: 3 -> 2 出现数据倾斜：P1: 1,2  P2: 3,4,5,6
    //val coalRDD = dataRDD.coalesce(2)
    // TODO: 2.如需数据均衡，需要重新shuffle 参数设置
    val coalRDD = dataRDD.coalesce(2,true)
    coalRDD.saveAsTextFile("spark-core/datas/output/")
  }

  /**
   * 增加分区 & shuffle数据
   * @param sc
   */
  def transformCoalesce03(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)
    // TODO: 1.增加分区需要重新shuffle数据(Spark扩大分区不用此算子！)
    //val coalRDD: RDD[Int] = dataRDD.coalesce(3, true)
    //coalRDD.saveAsTextFile("spark-core/datas/output/")
    // TODO: 2.Spark增加分区算子:repartition
    val repartRDD = dataRDD.repartition(3)
    repartRDD.saveAsTextFile("spark-core/datas/output/")
  }
}

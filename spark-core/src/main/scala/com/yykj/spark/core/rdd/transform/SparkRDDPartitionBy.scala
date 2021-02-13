package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparkRDDPartitionBy {

  /**
   * partitionBy:标准操作[Transform & Key-Value类型] Key-Value类型：数据源必须是key-value类型
   * 将数据按照指定Partitioner重新进行分区。Spark默认的分区器是HashPartitioner
   * 可以自定义分区器改变数据存放规则
   * def partitionBy(partitioner: Partitioner): RDD[(K, V)]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 partitionBy:将数据按照指定Partitioner重新进行分区。Spark默认的分区器是HashPartitioner
    transformPartitionBy(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * partitionBy:将数据按照指定Partitioner重新进行分区。Spark默认的分区器是HashPartitioner
   * @param sc
   */
  def transformPartitionBy(sc: SparkContext): Unit = {
    //val dataRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    val dataRDD = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD = dataRDD.map((_, 1))
    val partRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))
    partRDD.saveAsTextFile("spark-core/datas/output/")
  }
}

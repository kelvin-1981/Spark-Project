package com.yykj.spark.core.rdd.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkRDDPartitioner {

  /**
   * Spark 分区器：Spark目前支持Hash分区和Range分区，和用户自定义分区。Hash分区为当前的默认分区。
   * 分区器直接决定了RDD中分区的个数、RDD中每条数据经过Shuffle后进入哪个分区，进而决定了Reduce的个数。
   * 1.只有Key-Value类型的RDD才有分区器，非Key-Value类型的RDD分区的值是None
   * 2.每个RDD的分区ID范围：0 ~ (numPartitions - 1)，决定这个值是属于那个分区的。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc: SparkContext = new SparkContext(conf)

    // TODO: 2.业务逻辑
    // 需求：将nba放入一个分区，cba、wnba分别放入两个分区
    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("nba", "xxx"), ("cba", "xxx"), ("wnba", "xxx"), ("nba", "xxx")),2)
    val partRDD: RDD[(String, String)] = dataRDD.partitionBy(new myPartitioner)
    partRDD.saveAsTextFile("spark-core/datas/output")

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * 自定义分区器
   * 1.继承Partitioner
   * 2.重写方法
   */
  class myPartitioner extends Partitioner{

    /**
     * 设置分区数量
     * @return
     */
    override def numPartitions: Int = {
      3
    }

    /**
     * 返回数据分区索引 从0开始
     * @param key：key-value类型的key
     * @return
     */
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}



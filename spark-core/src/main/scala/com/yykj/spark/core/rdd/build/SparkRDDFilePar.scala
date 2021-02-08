package com.yykj.spark.core.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取文件分区&并行度
 */
object SparkRDDFilePar {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(conf)

    // TODO: 2.textFile分区规则（实际分区数量与minParitions不一致）
    // 2.1 数据情况
    // 1) 3.txt 数据显示：1 2 3 字节(7个): 1\r\n 2\r\n 3

    // 2.2.1.textFile
    // 1) minPartitions:最小分区数量(默认2) math.min(defaultParallelism, 2)
    // 2.2.2.分区数量计算方式 hadoopFile->TextInputFormat->FileInputFormat->getSplits()
    // 1) totalSize:字节总数
    // 2) goalSize(每个分区的字节数量) = totalSize / (minPartitions或者传入的分区数)
    // 3) 分区数量 = totalSize / goalSize (7 / 2 = 3)

    // TODO: 3.textFile数据分配规则
    // 3.1 Spark读取文件采用Hadoop读取方式 以行为单位进行读取（与字节无关）
    // 3.2 数据读取以偏移量为单位 & 偏移量不会被重复读取
    // 1\r\n 偏移量： 0 1 2
    // 2\r\n 偏移量： 3 4 5
    // 3 偏移量： 6
    // 3.3 分区数量偏移量计算(偏移量默认+3)
    // 分区1: [0,3] => 1 \r \n   *: 1 \r \n 2 =>以为spark一行读取故实际读取：1 \r \n 2 \r \n
    // 分区2: [3,6] => 2 \r \n   *: 3
    // 分区3: [6,7] => 3         *: null
    val rdd: RDD[String] = sc.textFile("spark-core/datas/input/word-data/3.txt",2)
    rdd.saveAsTextFile("spark-core/datas/output/")

    // TODO: 3.关闭环境
    sc.stop()
  }
}

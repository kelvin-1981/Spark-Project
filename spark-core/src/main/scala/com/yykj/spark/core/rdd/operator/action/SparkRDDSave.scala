package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDSave {

  /**
   * saveAsTextFile、saveAsObjectFile、saveAsSequenceFile:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 分将数据保存到不同格式的文件中
   * def saveAsTextFile(path: String): Unit
   * def saveAsObjectFile(path: String): Unit
   * def saveAsSequenceFile(path: String,codec: Option[Class[_ <: CompressionCodec]] = None): Unit
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 saveAsTextFile:保存数据至文件
    //actionSaveAsTextFile(sc)
    // TODO: 2.2 actionSaveAsObjectFile:保存数据对象至文件
    //actionSaveAsObjectFile(sc)
    // TODO: 2.3 saveAsSequenceFile:保存数据对象至文件 数据元素必须为Key-Value类型
    actionSaveAsSequenceFile(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * saveAsTextFile:保存数据至文件
   * @param sc
   */
  def actionSaveAsTextFile(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    dataRDD.saveAsTextFile("spark-core/datas/output/")
  }

  /**
   * actionSaveAsObjectFile:保存数据对象至文件
   * @param sc
   */
  def actionSaveAsObjectFile(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    dataRDD.saveAsObjectFile("spark-core/datas/output/")
  }

  /**
   * saveAsSequenceFile:保存数据对象至文件
   * @param sc
   */
  def actionSaveAsSequenceFile(sc: SparkContext): Unit = {
    val datRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)
    datRDD.saveAsSequenceFile("spark-core/datas/output/")
  }
}

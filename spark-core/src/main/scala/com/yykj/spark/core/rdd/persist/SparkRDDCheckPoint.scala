package com.yykj.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCheckPoint {

  /**
   * 所谓的检查点其实就是通过将RDD中间结果写入磁盘
   * 由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后有节点出现问题，可以从检查点开始重做血缘,减少开销。
   * 对RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc: SparkContext = new SparkContext(conf)
    // TODO: 2.设置检查点路径
    sc.setCheckpointDir("spark-core/datas/checkdir")

    // TODO: 2.业务逻辑
    /** 缓存和检查点区别
     * 1）Cache：只是将数据临时存储至内存中进行数据重用；Cache不切断血缘关系
     *          问题1：容易出现内存数据丢失、内存溢出等安全性问题
     * 2）Persist：将数据临时存储至磁盘中进行数据重用，作业执行完毕，临时数据文件丢失；Cache不切断血缘关系
     *          问题1：涉及磁盘IO，性能较低，但数据安全
     * 3）CheckPoint：将数据长久地保存至磁盘进行数据重用，CheckPoint切断血缘关系
     *          问题1：涉及磁盘IO，性能较低，但数据安全
     *          问题2：为保障数据安全一般会独立执行作业，效率低.解决此问题需要cache与checkpoint联合使用
     */
    val list: List[String] = List("hello spark", "hello scala")
    val dataRDD: RDD[String] = sc.makeRDD(list,2)
    val flatRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("process...")
      (word,1)
    })
    // TODO: 2.1 checkpoint: 需要落盘，需指定检查点保存路径
    // 路径中保存文件 作业执行完毕后不会被删除
    // 一般保存路径都存放于分布式存储中 如:HDFS
    // TODO: 问题：checkpoint为保障数据安全，会单独执行作业，导致mapRDD计算两边，效果低下
    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    // TODO: 3.关闭环境
    sc.stop()
  }
}

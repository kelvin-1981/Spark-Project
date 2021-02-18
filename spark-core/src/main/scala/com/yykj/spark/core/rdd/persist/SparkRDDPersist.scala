package com.yykj.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDPersist {

  /**
   * RDD持久化 : RDD中不存储数据。如一个RDD需要重复使用，需要从头再次执行获取数据（实现对象重用未实现数据重用），解决此问题需要RDD持久化
   * RDD通过Cache或者Persist方法将前面的计算结果缓存，默认情况下会把数据以缓存在JVM的堆内存中。但是并不是这两个方法被调用时立即缓存，
   * 而是触发后面的action算子时，该RDD将会被缓存在计算节点的内存中，并供后面重用。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc: SparkContext = new SparkContext(conf)

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

    // TODO: 2.1 persist : 可以选择缓存方式及位置
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    // TODO: 3.关闭环境
    sc.stop()
  }
}

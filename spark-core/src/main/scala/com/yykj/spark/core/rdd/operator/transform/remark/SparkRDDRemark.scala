package com.yykj.spark.core.rdd.operator.transform.remark

import com.yykj.spark.core.mock.SparkMockContext
import com.yykj.spark.core.rdd.operator.transform.SparkRDDCombineByKey.transformCombineByKey
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SparkRDDRemark {

  /**
   * 1 reduceByKey、foldByKey、aggregateByKey、combineByKey的区别
   * 1.1 ReduceByKey: 相同key的第一个数据不进行任何计算，分区内和分区间计算规则相同
   * 1.2 FoldByKey: 相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
   * 1.3 AggregateByKey：相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
   * 1.4 CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.reduceByKey、foldByKey、aggregateByKey、combineByKey的区别
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4),("b", 5),("a", 6)), 2)

    // TODO: 2.1 ReduceByKey:相同key的第一个数据不进行任何计算，分区内和分区间计算规则相同
    val reduceRDD: RDD[(String, Int)] = dataRDD.reduceByKey(_ + _)
    println("reduceByKey: " + reduceRDD.collect().mkString(","))
    // TODO: 2.2 FoldByKey: 相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
    val foldRDD = dataRDD.foldByKey(0)(_ + _)
    println("foldByKey: " + foldRDD.collect().mkString(","))
    // TODO: 2.3 AggregateByKey：相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
    val aggRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(_ + _, _ + _)
    println("aggregateByKey: " + aggRDD.collect().mkString(","))
    // TODO: 2.4 CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。
    val combRDD = dataRDD.combineByKey(v => v, (x1: Int, x2: Int) => (x1 + x2), (x1: Int, x2: Int) => (x1 + x2))
    println("aggregateByKey: " + aggRDD.collect().mkString(","))

    // TODO: 3.底层调用方法相同
    /*
       reduceByKey:
            combineByKeyWithClassTag[V](
                (v: V) => v, // 第一个值不会参与计算
                func, // 分区内计算规则
                func, // 分区间计算规则
                )

       aggregateByKey :
           combineByKeyWithClassTag[U](
               (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
               cleanedSeqOp, // 分区内计算规则
               combOp,       // 分区间计算规则
               )

       foldByKey:
           combineByKeyWithClassTag[V](
               (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
               cleanedFunc,  // 分区内计算规则
               cleanedFunc,  // 分区间计算规则
               )

       combineByKey :
           combineByKeyWithClassTag(
               createCombiner,  // 相同key的第一条数据进行的处理函数
               mergeValue,      // 表示分区内数据的处理函数
               mergeCombiners,  // 表示分区间数据的处理函数
               )
        */

    // TODO: 4.关闭环境
    sc.stop()
  }
}

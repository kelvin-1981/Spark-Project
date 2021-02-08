package com.yykj.spark.core.mock

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.spark.rdd.{HadoopRDD, ParallelCollectionRDD, RDD}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.Map
import scala.reflect.ClassTag

class SparkMockContext {

  /**
   * 默认分区数量
   */
  var defaultParallelism: Int = 3

  /**
   * 最小分区数量
   */
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

  /**
   * 读取数据
   * @param path
   */
  def textFile(path : String) : Seq[Int] = {
    null
  }

  /**
   *
   * @param seq
   * @param numSlices
   * @tparam T
   * @return
   */
  def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism, f : (Int) => Int): SparkMockRDD[T] = {
    parallelize(seq, numSlices,f)
  }

  /**
   *
   * @param seq
   * @param numSlices
   * @tparam T
   * @return
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism, f : (Int) => Int): SparkMockRDD[T] = {
    new SparkMockParallelRDD(this,seq, numSlices, f)
  }

  /**
   * 读取文件
   * @param path
   * @param minPartitions
   * @return
   */
  def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
//    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
//      minPartitions).map(pair => pair._2.toString).setName(path)
    null
  }

  /**
   * 执行计算任务
   * @param rdd
   * @param func
   * @param partitions
   * @tparam T
   * @tparam U
   * @return
   */
//  def runJob[T, U: ClassTag](rdd: SparkMockRDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
//    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
//  }
  def runJob[T, U: ClassTag](rdd: SparkMockRDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
    //runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
    null
  }
}

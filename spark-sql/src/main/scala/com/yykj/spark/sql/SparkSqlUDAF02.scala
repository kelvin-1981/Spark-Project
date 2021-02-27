package com.yykj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}


object SparkSqlUDAF02 {

  /**
   * Spark-SQL 自定义聚合函数
   * 1.继承UserDefinedAggregateFunction 实现聚合计算
   * 问题：此种方法使用索引的弱类型进行操作
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO: 2.业务逻辑
    // TODO: 2.1 Spark-SQL 自定义聚合函数,计算年龄平均值
    SparkSqlUDAF01(spark)

    // TODO: 3.关闭环境
    spark.stop()
  }

  /**
   * Spark-SQL 自定义聚合函数
   * @param spark
   */
  def SparkSqlUDAF01(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.createOrReplaceTempView("user")

    //spark.udf.register("ageAvg", functions.udaf(new MyAvgClass))
    //spark.sql("select ageAvg(age) from user").show()
  }

  /**
   * 自定义缓冲类
   * @param sum
   * @param totle
   */
  case class Buff(var sum: Long, var totle: Long)

  /**
   * 自定义聚合函数:Aggregator
   * IN：输入数据类型
   * Buffer： 缓冲区类型
   * OUT：输出数据类型
   */
  class MyAvgClass extends Aggregator[Long,Buff,Long] {
    /**
     * 缓冲区：初始值
     * @return
     */
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    /**
     * 根据数据更新缓冲区数据
     * @param b
     * @param a
     * @return
     */
    override def reduce(b: Buff, a: Long): Buff = {
      b.sum += a
      b.totle += 1
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.totle += b2.totle
      b1
    }

    override def finish(reduction: Buff): Long = {
      reduction.sum / reduction.totle
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}

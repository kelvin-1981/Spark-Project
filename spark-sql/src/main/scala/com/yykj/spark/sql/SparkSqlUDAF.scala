package com.yykj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSqlUDAF {

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

    spark.udf.register("ageAvg", new MyAvgClass)
    spark.sql("select ageAvg(age) from user").show()
  }

  /**
   * 自定义聚合函数
   */
  class MyAvgClass extends UserDefinedAggregateFunction {
    /**
     * IN：输入值类型
     * @return
     */
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    /**
     * 缓冲区数据类型
     * @return
     */
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("sum", LongType),
          StructField("count", LongType)
        )
      )
    }

    /**
     * OUT: 返回值数据类型
     * @return
     */
    override def dataType: DataType = LongType

    /**
     * 函数稳定性： 传入相同的参数 返回结果是否一致 （解决随机数计算问题）
     * @return
     */
    override def deterministic: Boolean = true

    /**
     * 缓冲区初始化
     * @param buffer
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    /**
     * 每行数据的更新逻辑
     * @param buffer
     * @param input
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getLong(0)
      val count = buffer.getLong(1)
      buffer.update(0, sum + input.getLong(0))
      buffer.update(1, count + 1)
    }

    /**
     * 数据合并
     * @param buffer1
     * @param buffer2
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    /**
     * 计算最终结果
     * @param buffer
     * @return
     */
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}

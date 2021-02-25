package com.yykj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test {

  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO: 2.业务逻辑
    // TODO: 2.1 基于文件创建DataFrame 
    //readFileToDataFrame(spark)
    // TODO: 2.2 SQL语法实现数据查询
    //sqlToDataFrame(spark)
    // TODO: 2.3 DSL语法实现数据查询
    //dslToDataFrame(spark)
    // TODO: 2.4 RDD转换DataFrame
    //rddToDataFrame(spark)
    // TODO: 2.5 DataFrame转换为RDD
    dataFrameToRDD(spark)

    // TODO: 3.关闭环境
    spark.stop()
  }

  /**
   * 读取文件生成DataFrame
   * @param spark
   */
  def readFileToDataFrame(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.show()
  }

  /**
   * SQL 语法实现数据查询
   * @param spark
   */
  def sqlToDataFrame(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    // TODO: 创建临时表 此表作用域为当前Session 
    df.createOrReplaceTempView("user")
    //df.createGlobalTempView("user")
    val sqlDF = spark.sql("select name from user")
    sqlDF.show()
  }

  /**
   * DSL 语法实现数据查询
   * @param spark
   */
  def dslToDataFrame(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    // TODO: 查询单列
    //df.select("name").show()
    // TODO: 查询多列
    //df.select("name","age").show()

    import spark.implicits._
    // TODO: 查询age年龄+1 $使用前提：import spark.implicits._
    //df.select($"name", $"age" + 1 ).show()
    // TODO: 过滤条件
    //df.filter($"age" > 30).show()
    // TODO: 分组数据
    df.groupBy("sex").count().show()
  }

  /**
   * RDD转换为DataFrame
   * @param spark
   */
  def rddToDataFrame(spark: SparkSession): Unit = {
    val list = List(("U001", 29, "M"), ("U002", 59, "M"), ("U003", 59, "M"), ("U004", 26, "M"), ("U005", 22, "M"))
    val dataRDD: RDD[(String, Int, String)] = spark.sparkContext.makeRDD(list)
    import spark.implicits._
    val df: DataFrame = dataRDD.toDF("name","age","sex")
    df.show()
  }

  /**
   * DataFrame转换为RDD
   * @param spark
   */
  def dataFrameToRDD(spark: SparkSession): Unit = {
    // TODO: 1.生成DataFrame 
    val list = List(("U001", 29, "M"), ("U002", 59, "M"), ("U003", 59, "M"), ("U004", 26, "M"), ("U005", 22, "M"))
    val dataRDD: RDD[(String, Int, String)] = spark.sparkContext.makeRDD(list)
    import spark.implicits._
    val df: DataFrame = dataRDD.toDF("name","age","sex")

    // TODO: 2.DF转换回RDD
    val rdd: RDD[Row] = df.rdd
  }
}

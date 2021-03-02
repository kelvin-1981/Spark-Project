package com.yykj.spark.sql.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlUDF {
  /**
   * Spark-SQL 自定义函数
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO: 2.业务逻辑
    // TODO: 2.1 创建Spark-SQL自定义函数：单列运算
    //SparkSqlUDF01(spark)
    // TODO: 2.2 创建Spark-SQL自定义函数：多列运算
    SparkSqlUDF02(spark)


    // TODO: 3.关闭环境
    spark.stop()
  }

  /**
   * UDF:Spark-SQL自定义函数：单列运算
   *
   * @param spark
   */
  def SparkSqlUDF01(spark: SparkSession): Unit = {

    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefix", (name: String) => {
      "Name: " + name
    })
    spark.sql("select prefix(name), age from user").show()
  }

  /**
   * UDF:Spark-SQL自定义函数: 多列运算
   *
   * @param spark
   */
  def SparkSqlUDF02(spark: SparkSession): Unit = {

    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("union", (user: String, age: Int) => {
      user + " & " + age
    })
    spark.sql("select union(name, age) as union from user").show()
  }
}

package com.yykj.spark.sql.loadwrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 读取MYSQL数据库
 */
object SparkSqlMySQL {

  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO: 2.业务逻辑
    // TODO: 2.1 Spark-SQL读取MySQL DB
    //loadMySQL(spark)
    // TODO: 2.2 读取MySQL数据库 查询数据
    loadAndWriteMySQL(spark)

    // TODO: 3.关闭环境
    spark.stop()
  }

  /**
   * 读取MySQL数据库 查询数据
   * @param spark
   */
  def loadMySQL(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost/testdb")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "hh96n55g")
      .option("dbtable", "student")
      .load()
    df.createOrReplaceTempView("student")

    spark.sql("select * from student where age < 40").show()
  }

  /**
   * 读取MySQL数据库 & 保存数据
   * @param spark
   */
  def loadAndWriteMySQL(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost/testdb")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "hh96n55g")
      .option("dbtable", "student")
      .load()

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost/testdb")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "hh96n55g")
      .option("dbtable", "student_temp")
      .mode(SaveMode.Append)
      .save()


  }

}

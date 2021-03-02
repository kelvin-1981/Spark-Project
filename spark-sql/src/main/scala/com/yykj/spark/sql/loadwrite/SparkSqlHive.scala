package com.yykj.spark.sql.loadwrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Spark-SQL操作HIVE
 */
object SparkSqlHive {

  /**
   * 此功能未实现
   * @param args
   */
  def main(args: Array[String]): Unit = {

    //System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setMaster("local[*]").setAppName("SS")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // TODO: 缺少spark环境
    spark.sql("show databases").show()

    spark.stop()
  }
}

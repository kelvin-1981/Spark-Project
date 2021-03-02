package com.yykj.spark.sql.loadwrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * spark.read.load 是加载数据的通用方法
 * df.write.save 是保存数据的通用方法
 * ➢ format("…")：指定加载的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"。
 * ➢ load("…")：在"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"格式下需要传入加载数据的路径。
 * ➢ option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable
 */
object SparkSqlLoadWrite {

  /**
   * Spark SQL 数据装载及读取
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // TODO: 2.业务逻辑
    // TODO: 2.1 Spark-SQL读取parquet文件
    //loadParquetFile(spark)
    // TODO: 2.2 Spark-SQL读取json文件
    //loadJsonFile(spark)
    // TODO: 2.4 Spark-SQL读取CSV文件
    loadCsvFile(spark)

    // TODO: 2.3 Spark-SQL写文件
    //writeFile(spark)
    // TODO: 2.4 Spark-SQL写文件模式
    //writeSaveMode(spark)


    // TODO: 3.关闭环境
    spark.stop()
  }

  /**
   * Spark-SQL读取parquet文件
   * @param spark
   */
  def loadParquetFile(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.load("spark-sql/datas/input/users.parquet")
    df.show()
  }

  /**
   * Spark-SQL读取json文件
   * @param spark
   */
  def loadJsonFile(spark: SparkSession): Unit = {
    // TODO: 1. 直接调用Read.json方法
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.show()

    // TODO: 2. 调用Load方法(其他格式通用)
    val df_2 = spark.read.format("json").load("spark-sql/datas/input/user.json")
    df_2.show()

    // TODO: 3. SQL语句直接读取文件(其他格式通用)
    val df_3: DataFrame = spark.sql("select * from json.`spark-sql/datas/input/user.json`")
    df_3.show()
  }

  /**
   * Spark-SQL读取csv文件
   * @param spark
   */
  def loadCsvFile(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.format("csv").option("seq", ",").load("spark-sql/datas/input/people.csv")
    df.show()
  }

  /**
   * Spark-SQL写文件
   * @param spark
   */
  def writeFile(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.write.format("json").save("spark-sql/datas/output")
  }

  /**
   * Spark-SQL写文件模式 SaveMode是一个枚举类，其中的常量包括： Scala/Java Any Language Meaning
   * 1.SaveMode.ErrorIfExists(default) 如果文件已经存在则抛出异常 SaveMode.Append "append" 如果文件已经存在则追加
   * 2.SaveMode.Append 如果文件已经存在则追加
   * 3.SaveMode.Overwrite 如果文件已经存在则覆盖 SaveMode.Ignore "ignore" 如果文件已经存在则忽略
   * 4.SaveMode.Ignore 如果文件已经存在则忽略
   * @param spark
   */
  def writeSaveMode(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/datas/input/user.json")
    df.write.mode("append").format("json").save("spark-sql/datas/output")
  }
}

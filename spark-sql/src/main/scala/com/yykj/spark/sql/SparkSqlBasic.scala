package com.yykj.spark.sql

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 1. RDD: RDD是一种分布式数据结构。 *RDD只关注数据(数据类型)
 * 2. DataFrame: DataFrame是一种以RDD为基础的分布式数据集，类似于传统数据库中的二维表格。 *DatFrame关注数据 & 数据结构
 * 3. DataSet: DataSet是分布式数据集合 *DataSet关注数据 & 数据结构 & 业务类型(转换的业务类型)
 */
object SparkSqlBasic {

  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //import spark.implicits._

    // TODO: 2.业务逻辑
    // TODO: 2.1 DataFrame:基于文件创建DataFrame DataFrame是DataSet的特定泛型DataSet[ROW]
    //readFileToDataFrame(spark)
    // TODO: 2.2 DataFrame:SQL语法实现数据查询
    //sqlToDataFrame(spark)
    // TODO: 2.3 DataFrame:DSL语法实现数据查询
    //dslToDataFrame(spark)

    // TODO: 2.4 RDD转换DataFrame
    //rddToDataFrame(spark)
    // TODO: 2.5 DataFrame转换为RDD
    //dataFrameToRDD(spark)

    // TODO: 2.6 DataSet:列表创建
    //arrayToDataSet(spark)

    // TODO: 2.7 DataFrame转换为 DataSet
    //dataFrameToDataSet(spark)
    // TODO: 2.8 DataSet转换为DataFrame
    //dataSetToDataFrame(spark)

    // TODO: 2.9 RDD转换为DataSet
    rddToDataSet(spark)
    // TODO: 2.10 DataSet转换为RDD
    //dataSetToRDD(spark)

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
    spark.sql("select name from user").show()
    spark.sql("select name, age from user").show()
    spark.sql("select avg(age) from user").show()
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

    // TODO: spark为对象名称
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
    // TODO: RDD转换DatFrame时需要指定数据结构(列名)
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

  /**
   * 列表创建DataSet
   * @param spark
   */
  def arrayToDataSet(spark: SparkSession): Unit = {
    val seq: Seq[Int] = Seq(1, 2, 3, 4, 5)
    import spark.implicits._
    val ds: Dataset[Int] = seq.toDS()
    ds.show()
  }

  /**
   * DataFrame转换为 DataSet
   * @param spark
   */
  def dataFrameToDataSet(spark: SparkSession): Unit = {
    val list = List(("U001", 29, "M"), ("U002", 59, "M"), ("U003", 59, "M"), ("U004", 26, "M"), ("U005", 22, "M"))
    val dataRDD: RDD[(String, Int, String)] = spark.sparkContext.makeRDD(list)
    import spark.implicits._
    val df: DataFrame = dataRDD.toDF("name","age","sex")

    //转换DataSet
    val ds: Dataset[User] = df.as[User]
    ds.foreach(user => {
      println(user.name + " | " + user.age)
    })
  }

  /**
   * DataSet转换为DataFrame
   * @param spark
   */
  def dataSetToDataFrame(spark: SparkSession): Unit = {
    val list = List(("U001", 29, "M"), ("U002", 59, "M"), ("U003", 59, "M"), ("U004", 26, "M"), ("U005", 22, "M"))
    import spark.implicits._
    val ds = list.toDS()

    val df = ds.toDF()
    df.show()
    //df.printSchema()
  }

  /**
   * RDD转换为DataSet
   * @param spark
   */
  def rddToDataSet(spark: SparkSession): Unit = {
    import spark.implicits._
    val dataRDD: RDD[(String, Int, String)] = spark.sparkContext.makeRDD(List(("U001", 29, "M"), ("U002", 59, "M"), ("U003", 59, "M")))
    val ds: Dataset[User] = dataRDD.map {
      case (name, age, sex) => User(name, age, sex)
    }.toDS()

    ds.show()
  }

  /**
   * RDD转换为DataSet
   * @param spark
   */
  def dataSetToRDD(spark: SparkSession): Unit = {
    val dataRDD: RDD[User] = spark.sparkContext.makeRDD(List(User("U001", 29, "M"), User("U002", 59, "M"), User("U003", 59, "M")))

    import spark.implicits._
    val ds: Dataset[User] = dataRDD.toDS()

    val rdd: RDD[User] = ds.rdd
    rdd.collect().foreach(println)
  }

  case class User(var name: String,var age: Int, var sex: String)
}


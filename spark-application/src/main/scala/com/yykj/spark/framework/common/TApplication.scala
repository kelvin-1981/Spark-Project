package com.yykj.spark.framework.common

import com.yykj.spark.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

/**
 * trait:特征  类似于Java接口
 * 用途： 进行通用环境的创建及配置
 */
trait TApplication {

  /**
   * 执行应用程序
   * op: => Unit: Scala传递逻辑代码
   */
  def start(op: => Unit): Unit = {

    // TODO: 1.声明环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // TODO: 2.控制层对象 调用调度控制方法
    op

    // TODO: 3.关闭环境
    sc.stop()
  }
}

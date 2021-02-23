package com.yykj.spark.framework.common

import com.yykj.spark.framework.util.EnvUtils
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master: String = "local[*]", appName: String = "WordCount", op: => Unit): Unit ={
    // TODO: 1.声明环境
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    // TODO: 2.将环境对象保存至threadlocal 内存保存
    EnvUtils.put(sc)

    // TODO: 2.控制层对象 调用调度控制方法
    op

    // TODO: 3.关闭环境
    sc.stop()
  }
}

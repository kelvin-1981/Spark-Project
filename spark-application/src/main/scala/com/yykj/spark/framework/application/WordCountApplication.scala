package com.yykj.spark.framework.application

import com.yykj.spark.framework.common.TApplication
import com.yykj.spark.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 展现层[Application]
 * @param
 */
object WordCountApplication extends App with TApplication{

  start{
    // TODO: 控制层对象 调用调度控制方法
    var controller = new WordCountController()
    controller.dispatch()
  }
}

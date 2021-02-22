package com.yykj.spark.framework.controller

import com.yykj.spark.framework.service.WordCountService
import org.apache.spark.rdd.RDD

/**
 * 控制层[Controller]
 *
 * @param
 */
class WordCountController {

  // TODO: 服务层对象
  private var service = new WordCountService()

  /**
   * 调用服务层对象 进行业务逻辑计算
   */
  def dispatch(): Unit = {
    // TODO: 1.调用service层对象 获取计算结果
    val arr: Array[(String, Int)] = service.dataAnalysis()
    // TODO: 2.输出结果
    arr.foreach(println)
  }
}

package com.yykj.spark.framework.controller

import com.yykj.spark.framework.common.TController
import com.yykj.spark.framework.service.WordCountService

class WordCountController extends TController{

  private var service = new WordCountService()

  override def dispatch(): Unit = {
    val arr:Array[(String, Int)]= service.dataAnalysis()
    arr.foreach(println)
  }
}

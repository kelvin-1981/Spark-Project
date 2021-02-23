package com.yykj.spark.framework.application

import com.yykj.spark.framework.common.TApplication
import com.yykj.spark.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  //TODO: 调用TApplication实现的start方法
  start("local[*]","WordCount",op = {
    var controller = new WordCountController()
    controller.dispatch()
  })
}

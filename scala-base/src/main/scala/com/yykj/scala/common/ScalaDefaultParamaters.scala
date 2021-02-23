package com.yykj.scala.common

object ScalaDefaultParamaters {

  /**
   * 方法的默认参数及用法
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.不传递参数
    //println(defaultParamaters())
    // TODO: 2.传递部分参数()
    println(defaultParamaters(10,20))
    // TODO: 3.传递全部参数
    //println(defaultParamaters(10, 20, 30))
  }

  /**
   * 方法参数定制默认值
   * @param x
   * @param y
   * @return
   */
  def defaultParamaters(x: Int = 1, y: Int = 2, z:Int = 3): Int = {
    x + y + z
  }
}

package com.yykj.scala.list

object ScalaList {

  /**
   * list listbuffer
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 创建
    val list = List(1, 2, 3,"hello")
    println(list)

    // TODO: 使用
    println(list(0))

    for(i <- list){
      println(i)
    }
  }
}

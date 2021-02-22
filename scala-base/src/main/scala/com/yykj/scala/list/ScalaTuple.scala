package com.yykj.scala.list

object ScalaTuple {

  /**
   * 元组：将多个无关数据封装为一个整体 灵活
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 为了高效操作元组，编译器根据元素数量不同，对应不同的元组类型
    //tuple 1-22 , 下面对应tuple5类型
    val tup = (1, 2, 3, "hello", 4)
    println(tup)

    // TODO: 元组元素
    println(tup._1)
    println(tup.productElement(0))

    // TODO: 遍历 
    for(i <- tup.productIterator){
      println(i)
    }
  }

}

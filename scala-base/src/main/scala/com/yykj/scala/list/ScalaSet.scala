package com.yykj.scala.list

import scala.collection.immutable
import scala.collection.mutable

object ScalaSet {

  /**
   * Set: 唯一、无序、底层使用哈希实现
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 一.创建
    // TODO: 1.创建 默认不可变
    val set_1 = Set(1, 2, 3)
    println(set_1)

    // TODO: 2.可变
    val set_2 = mutable.Set(1, 2, 3)
    println(set_2)

    //二、元素
    // TODO: 1.添加
    set_2.add(4)
    println(set_2)

    // TODO: 2.删除
    set_2.remove(4)
    println(set_2)

    // TODO: max min
    println(set_2.max)
    println(set_2.min)

    //三、遍历
    // TODO: 1.遍历
    for(i <- set_2){
      println(i)
    }

  }
}

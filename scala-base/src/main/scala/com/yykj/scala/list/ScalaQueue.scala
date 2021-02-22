package com.yykj.scala.list

import scala.collection.mutable

object ScalaQueue {

  /**
   * 队列
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 创建
    val que_1 = new mutable.Queue[Int]
    println(que_1)

    // TODO: 添加
    que_1 += 1
    que_1 += 2
    que_1 += 3
    println(que_1)

    // TODO: 出队
    val elem = que_1.dequeue()
    println(elem)
    println(que_1)

    // TODO: 入队
    val elem2 = que_1.enqueue(4, 5, 6)
    println(elem2)
    println(que_1)

    // TODO: 获取元素
    println(que_1.head)
    println(que_1.last)
    println(que_1.tail)
    println(que_1.tail.tail)

  }
}

package com.yykj.scala.spark

object ScalaSuperMap02 {

  /**
   * list内元素*2
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.map方法：遍历集合内所有元素
    // 计算： 1.遍历list 2.一次执行sum 3.计算结果放入新的list集合
    val list_1 = List(1, 2, 3, 4, 5)
    val list_2 = list_1.map(sum)
    println(list_2)

    println("----------------------")

    // TODO: 2.高阶函数
    // 1. _代表list内的元素
    val list_3 = list_1.map(_ * 2)
    println(list_3)
  }

  /**
   * n * 2
   * @param n
   * @return
   */
  def sum(n : Int) : Int = {
    n *  2
  }
}

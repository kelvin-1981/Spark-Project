package com.yykj.scala.spark

object ScalaSuperMap {
  /**
   * Scala函数话变成 Map映射 遍历集合元素
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.标准计算
    map_operator()

    // TODO: 2.高阶函数
    val res = high_fun(sum _ , 1)
    println(res)

  }

  /**
   * 高阶函数：可以将接受函数参数的方法
   * @param f
   * @param n
   */
  def high_fun(f : Double => Double, n : Double): Double = {
    f(n)
  }

  /**
   * 计算函数
   * @param d
   * @return
   */
   def sum(n : Double) : Double = {
     n * 2
   }

  /**
   * 计算
   */
  def map_operator() : Unit = {
    val list_1 = List(1, 2, 3, 4, 5)
    var list_2 = List[Int]()
    for(i <- list_1){
      list_2 = list_2 :+ i * 2
    }
    println(list_2)
  }


}

package com.yykj.scala.spark

object ScalaReduce {

  /**
   * reduce : 将集合中元素依次计算
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val list_1 = List(1, 2, 3, 4, 5)

    // TODO: reduce 集合中的元素一次计算 ((((1+2) + 3) + 4) + 5)
    val res_1 = list_1.reduce(sum)
    println(res_1)

    // TODO: reduce
    val res_2 = list_1.reduce(_ + _)
    println(res_2)

    // TODO:  
    println(list_1.reduceLeft(_ + _))
    println(list_1.reduceRight(_ + _))
  }

  /**
   * 计算方法
   * @param x1
   * @param x2
   * @return
   */
  def sum(x1 : Int,  x2 : Int) : Int = {
    x1 + x2
  }

}

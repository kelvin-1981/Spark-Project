package com.yykj.scala.function

object ScalaFunction {

  /**
   * ***方法与函数本质相同，定义不同位置而已
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val dog = new Dog

    // TODO: 1.调用方法
    println("method result : " + dog.sum(1,1))

    // TODO: 2.方法转换为函数
    val f1 = dog.sum _
    println("f1 : " + f1)

    // TODO: 3.调用函数
    println("f1 result : " + f1(100,100))

    // TODO: 4.定义函数
    val f2 = (x : Int, y : Int) => {
      x + y
    }
    println("f2 : " + f2)
    println("f2 result : " + f2(50,60))
  }
}

/**
 * 
 */
class Dog {

  /**
   * Method
   * @param x
   * @param y
   * @return
   */
  def sum(x : Int, y : Int): Int = {
    x + y
  }
}
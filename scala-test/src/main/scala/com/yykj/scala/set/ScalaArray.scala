package com.yykj.scala.set

/**
 * 定长数组
 */
object ScalaArray {
  def main(args: Array[String]): Unit = {

    // TODO: 声明int类型定长数组 
    val arr_1 = new Array[Int](4)
    println(arr_1.length)

    // TODO: 声明泛型定长数组
    val arr_2 = new Array[Any](4)
    arr_2(0) = 1
    arr_2(1) = "kelvin"
    println(arr_2(0) + " " + arr_2(1))
  }
}



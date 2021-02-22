package com.yykj.scala.list

/**
 * 定长数组
 */
object ScalaArray {
  def main(args: Array[String]): Unit = {

    // TODO: 声明int类型定长数组 
    val arr_1 = new Array[Int](3)
    println("arr_1: " + arr_1.length)

    // TODO: 直接初始化数组
    var arr_2 = Array("A","B","C")
    for(i <- arr_2){
      println(i)
    }

    // TODO: 声明泛型定长数组
    val arr_3 = new Array[Any](2)
    arr_3(0) = "kelvin"
    arr_3(1) = "tony"
    for(i <- 0 to arr_3.length - 1){
      println(arr_3(i))
    }
  }
}



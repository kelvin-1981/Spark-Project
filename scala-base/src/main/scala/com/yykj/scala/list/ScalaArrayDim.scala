package com.yykj.scala.list

object ScalaArrayDim {

  /**
   * 多维数组
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 创建
    val arr = Array.ofDim[Int](3, 4)
    print_list(arr)

    // TODO: 更改
    arr(0)(1) = 100
    print_list(arr)

  }

  /**
   * 打印
   * @param arr
   */
  def print_list(arr : Array[Array[Int]])={
    for(i <- arr){
      for(j <- i){
        print(j + "\t")
      }
      println()
    }
    println("------------------------------------")
  }
}

package com.yykj.scala.list

import scala.collection.mutable.ArrayBuffer

/**
 * 可变数组
 */
object ScalaArrayBuffer {

  /**
   * 
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 公有打印 
    var print_list = (arr:ArrayBuffer[Int]) => {
      println("start------------------------------")
      for(i <- arr){
        println(i)
      }
      println("end------------------------------")
    }

    // TODO: 声明数组
    var arr_1 = ArrayBuffer[Int](1,2,3)
    print_list(arr_1)

    // TODO: 添加单个元素
    arr_1.append(4)
    print_list(arr_1)

    // TODO: 添加多个元素
    arr_1.append(5,6,7)
    print_list(arr_1)

    // TODO: 更改元素
    arr_1(0) = 100;
    print_list(arr_1)

    // TODO: 删除元素
    arr_1.remove(0)
    print_list(arr_1)
  }
}

package com.yykj.scala.spark

object ScalaFilter {

  /**
   * 过滤
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val list_1 = List("kelvin", "tony", "sum")

    // TODO: filter：过滤：选出起始为K的元素
    val list_2 = list_1.filter(startChar)
    println(list_2)

    // TODO:
    val list_3 = list_1.filter(_.startsWith("k"))
    println(list_3)
  }

  /**
   * 判断其实值
   * @param str
   * @return
   */
  def startChar(str : String) : Boolean = {
    if(str.startsWith("k")){
      true
    }
    else{
      false
    }
  }
}

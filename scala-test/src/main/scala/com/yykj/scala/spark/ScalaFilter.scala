package com.yykj.scala.spark

object ScalaFilter {

  /**
   * 过滤
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 选出起始为K的元素
    val list_1 = List("kelvin", "tony", "sum")
    val list_2 = list_1.filter(startChar)
    println(list_2)
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

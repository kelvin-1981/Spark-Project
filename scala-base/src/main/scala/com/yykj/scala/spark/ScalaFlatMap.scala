package com.yykj.scala.spark

object ScalaFlatMap {

  /**
   * 扁平化处理 将集合中的每个元素的子元素映射到某个函数并返回新的集合
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val list_1 = List("kelvin", "tony", "sum")

    // TODO: flatmap：扁平化所有元素
    // TODO: 输出结果：List(K, E, L, V, I, N, T, O, N, Y, S, U, M)
    val list_2 = list_1.flatMap(upper)
    println(list_2)

    // TODO:
    val list_3 = list_1.flatMap(_.toUpperCase)
    println(list_3)
  }

  /**
   * 转换成为大写
   * @param s
   * @return
   */
  def upper(s : String) : String = {
    s.toUpperCase
  }
}

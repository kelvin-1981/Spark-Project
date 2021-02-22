package com.yykj.scala.spark

object ScalaTest {

  def main(args: Array[String]): Unit = {

    // TODO:
    val list_1 = List(1,2,3)
    for(i <- list_1){
      println(high_fun(sum _,i))
    }

    // TODO:
    val list_2 = List("kelvin", "tony", "sum")
    val list_3 = list_2.flatMap(_.toUpperCase)
    println(list_3)

    // TODO:
    val list_4 = list_2.filter(_.startsWith("k"))
    println(list_4)
  }

  def startWord(str : String) : Boolean = {
    if(str.startsWith("k")){
      true
    }
    else{
      false
    }
  }

  /**
   *
   * @param s
   * @return
   */
  def upper(s : String) : String = {
    s.toUpperCase()
  }

  /**
   *
   * @param f
   * @param num
   */
  def high_fun(f : Int => Int,num : Int) = {
    f(num)
  }

  /**
   *
   * @param num
   * @return
   */
  def sum(num : Int) : Int = {
    num * 2
  }
}

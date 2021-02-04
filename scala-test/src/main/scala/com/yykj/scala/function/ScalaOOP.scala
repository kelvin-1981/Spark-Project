package com.yykj.scala.function

object ScalaOOP {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val cat = new Cat
    cat.name = "小白"
    cat.age = 10
    cat.color = "White"
    print("Success：" + cat.name + " " + cat.age + " " + cat.color + " " + cat.compute())
  }
}

/**
 * Cat Class
 */
class Cat(){
  // TODO: 自动生成set get
  var name:String = ""
  var age:Int = _ //_:Int默认值为0
  var color:String = _ //_:string 默认值为“”
  var fun : (Int) => Int = {_ * 2}
  def compute() : Any = {
    fun(100)
  }
}

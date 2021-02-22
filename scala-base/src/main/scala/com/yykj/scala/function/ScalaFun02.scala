package com.yykj.scala.function

object ScalaFun02 {
  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO:定义方法一，有明确返回值类型 int
    println("getRes():" + getRes(1,1,"+"))

    // TODO:定义方法二：不定义明确返回值类型 int
    println("getRes2():" + getRes2(1,1,"*"))

    // TODO:定义方法三：无返回值（称之为过程）
    getRes3(1,1)
  }

  /**
   * 定义方法一，有明确返回值类型 int
   * @param v1
   * @param v2
   * @param oper
   * @return
   */
  def getRes(v1 : Int, v2 : Int, oper : String) : Int = {
    if(oper == "+"){
      v1 + v2
    }
    else if(oper == "-"){
      v1 - v2;
    }
    else{
      -10000;
    }
  }

  /**
   * 定义方法二：不定义明确返回值类型 int
   * @param v1
   * @param v2
   * @return
   */
  def getRes2(v1:Int,v2:Int,oper:String) = {
    if(oper == "+"){
      v1 + v2
    }
    else if(oper == "-"){
      v1 - v2;
    }
    else{
      null
    }
  }

  /**
   * 定义方法三：无返回值（称之为过程）
   * @param v1
   * @param v2
   */
  def getRes3(v1:Int,v2:Int) : Unit = {
    println("getRes2():" + (v1 + v2))
  }
}

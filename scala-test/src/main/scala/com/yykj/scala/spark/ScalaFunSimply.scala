package com.yykj.scala.spark

object ScalaFunSimply {

  /**
   * 匿名函数的简化写法
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.采用标准方式进行简化示例
    fun_simple_standrad()

    // TODO: 2.采用数组reduce进行简化示例
    //fun_simply_reduce()
  }

  /**
   * 标准示例
   */
  def fun_simple_standrad() = {

    // TODO: 1.标准
    val res_1 = fun_high_level((x: Int) => {
      x + 1
    })
    println("res_1: " + res_1)

    // TODO: 2.省略函数体的{} 在匿名函数中,如果函数体只有一行代码. 那么就可以把花括号给省略
    val res_2 = fun_high_level((x: Int) => x + 1)
    println("res_2: " + res_2)

    // TODO: 3.省略参数类型 在匿名函数中,可以省略类型
    val res_3 = fun_high_level( x => x + 1)
    println("res_3: " + res_3)

    // TODO: 4.在匿名函数中. 如果变量在函数体中只用过一次就可以把变量替换成_(下划线). 同时把=>给省略掉, 也省略=>前面的参数类型
    val res_4 = fun_high_level(_ + 1)
    println("res_4: " + res_4)

    // TODO: 5在匿名函数中, 如果有两个变量, 这两个变量各同时用过一次, 此时两个变量都替换成_(下划线). 同时把=>给省略掉.也把=>前面的类型给省略掉
    val res_5 = fun_high_level_02(_ + _)
    println("res_5: " + res_5)
  }

  /**
   * 标准 高阶函数 单个参数
   * @param f
   */
  def fun_high_level(f : (Int) => Int) : Int = {
    f(99)
  }

  /**
   * 标准 高阶函数 多个参数
   * @param f
   * @return
   */
  def fun_high_level_02(f : (Int,Int) => Int) : Int = {
    f(100,100)
  }

  /**
   * reduce 示例
   */
  def fun_simply_reduce() = {
    // TODO: 声明数组求和
    val list_1 = List(1, 2, 3, 4, 5)

    // TODO: 1.标准
    val res_1: Int = list_1.reduce(
      (x : Int, y : Int) => { x + y }
    )
    println("res_1: " + res_1)

    // TODO: 2.省略函数体的{} 在匿名函数中,如果函数体只有一行代码. 那么就可以把花括号给省略
    val res_2 = list_1.reduce((x: Int, y: Int) => x + y)
    println("res_2: " + res_2)

    // TODO: 3.省略参数类型 在匿名函数中,可以省略类型
    val res_3 = list_1.reduce((x, y) => x + y)
    println("res_3: " + res_3)

    // TODO: 4.1在匿名函数中. 如果变量在函数体中只用过一次就可以把变量替换成_(下划线). 同时把=>给省略掉, 也省略=>前面的参数类型
    // TODO: 4.2在匿名函数中, 如果有两个变量, 这两个变量各同时用过一次, 此时两个变量都替换成_(下划线). 同时把=>给省略掉.也把=>前面的类型给省略掉
    val res_4 = list_1.reduce(_ + _)
    println("res_4: " + res_4)
  }
}

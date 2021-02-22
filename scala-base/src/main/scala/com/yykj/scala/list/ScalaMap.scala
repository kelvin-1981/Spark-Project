package com.yykj.scala.list

import scala.collection.mutable
import scala.collection.immutable

object ScalaMap {

  /**
   * map: key value
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 一、构造Map
    // TODO: 1.创建不可变map,每一对数据为tuple2
    val map_1 = immutable.Map("1" -> 100, "2" -> 200, "3" -> 300)
    println("map_1: " +  map_1)

    // TODO: 2.创建可变map
    val map_2 = mutable.Map("1" -> 1000, "2" -> 2000, "3" -> 3000)
    println("map_2: " +  map_2)

    val map_4 = mutable.Map(("1", -1), ("2", -2), ("3", -3))
    println("map_4: " +  map_4)

    // TODO: 3.创建空map
    val map_3 = new mutable.HashMap[String, Int]
    println("map_3: " +  map_3)


    // 二、获取数据
    // TODO: 1.map(key)
    val value_1 = map_1("1")
    println(value_1)

    // TODO: 2.判断key
    if(map_1.contains("1")){
      println("key exites")
    }

    // TODO: 3.map.get(key)
    println(map_1.get("1").get)

    // TODO: 4.map.getOrElse
    println(map_1.getOrElse("A","fsfsfsd"))

    //三、修改元素
    // TODO: 1.添加key value
    map_2 += ("4" -> 4000)
    println(map_2)

    // TODO: 2.更改value
    map_2("4") = 9999
    println(map_2)

    // TODO: 删除元素
    map_2 -= "4"
    println(map_2)
    
    //四、遍历
    // TODO: 1.获取key value
    val map_6 = mutable.Map(("1", -1), ("2", -2), ("3", -3))
    for((k,v) <- map_6){
      println(k + " " + v)
    }

    // TODO: 2.获取key
    for(k <- map_6.keys){
      println(k)
    }
  }
}

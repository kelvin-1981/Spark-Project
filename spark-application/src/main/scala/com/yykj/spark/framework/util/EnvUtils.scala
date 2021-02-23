package com.yykj.spark.framework.util

import org.apache.spark.SparkContext

object EnvUtils {

  private var thread = new ThreadLocal[SparkContext]

  def put(sc: SparkContext): Unit = {
    thread.set(sc)
  }

  def get(): SparkContext = {
    thread.get()
  }

  def clear(): Unit = {
    thread.remove()
  }
}

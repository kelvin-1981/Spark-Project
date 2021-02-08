package com.yykj.spark.core.mock

import scala.reflect.ClassTag

class SparkMockParallelPartition[T : ClassTag](var rddID: Long, var slice: Int, var values: Seq[T])
  extends SparkMockPartition with Serializable {

  val iterator: Iterator[T] = values.iterator
}

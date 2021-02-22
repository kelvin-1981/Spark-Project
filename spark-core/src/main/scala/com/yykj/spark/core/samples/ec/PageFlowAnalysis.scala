package com.yykj.spark.core.samples.ec

import com.yykj.spark.core.samples.ec.HotCategorySessionAnalysis.getTop10HotCategory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageFlowAnalysis {

  def main(args: Array[String]): Unit = {

    // TODO: 1.需求描述
    /**
     * 计算页面单跳转化率，什么是页面单跳转换率，比如一个用户在一次 Session 过程中访问的页面路径 3,5,7,9,10,21，
     * 那么页面 3 跳到页面 5 叫一次单跳，7-9 也叫一次单跳，那么单跳转化率就是要统计页面点击的概率。
     * 比如：计算 3-5 的单跳转化率，先获取符合条件的 Session 对于页面 3 的访问次数（PV）为 A，
     * 然后获取符合条件的 Session 中访问了页面 3 又紧接着访问了页面 5 的次数为 B，那么 B/A 就是 3-5 的页面单跳转化率。
     */

    // TODO: 2.开发
    // TODO: 2.1 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("SC")
    val sc = new SparkContext(conf)

    // TODO: 2.2 业务逻辑
    // TODO: 2.2.1 获取数据
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/sample/user_visit_action.txt")

    // TODO: 2.2.2 转换成为用户对象
    val objDataRDD: RDD[UserVisitAction] = dataRDD.map(line => {
      val datas = line.split("_")
      UserVisitAction(datas(0), datas(1).toLong, datas(2), datas(3).toLong, datas(4), datas(5), datas(6).toLong,
        datas(7).toLong, datas(8), datas(9), datas(10), datas(11), datas(12).toLong)
    })
    objDataRDD.cache()

    // TODO 2.2.3: 统计分母 各页面的访问总数
    val pCntArr: Map[Long, Long] = objDataRDD.map(info => {
      (info.page_id, 1L)
    }).reduceByKey(_ + _).collect().toMap

    // TODO: 2.2.4 统计分子-根据Session进行分组
    val sessRDD: RDD[(String, Iterable[UserVisitAction])] = objDataRDD.groupBy(_.session_id)

    // TODO: 2.2.4 统计分子-根据时间进行升序排序 & 转换格式 & 计算
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessRDD.mapValues(iter => {
      val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
      //转换成为[(首页,详情),(详情,下单),(下单,支付)]格式
      val pageArr: List[Long] = sortList.map(_.page_id)
      val pageFlowIDs: List[(Long, Long)] = pageArr.zip(pageArr.tail)
      pageFlowIDs.map((_, 1))
    })
    val mvRDD_2: RDD[((Long, Long), Int)] = mvRDD.flatMap(_._2)
    val mvRDD_3: RDD[((Long, Long), Int)] = mvRDD_2.reduceByKey(_ + _)

    // TODO: 2.2.5 计算页面转换率
    mvRDD_3.foreach{
      case ((p1, p2), sum) => {
        val pCount = pCntArr.getOrElse(p1, 0L)
        println(s"页面${p1}跳转到页面${p2}单跳转换率为:" + ( sum.toDouble/pCount))
      }
    }


    // TODO: 3 环境关闭
    sc.stop()
  }

  case class UserVisitAction(
      date: String, // 用户点击行为的日期
      user_id: Long, // 用户的 ID
      session_id: String, //Session 的 ID
      page_id: Long, // 某个页面的 ID
      action_time: String, // 动作的时间点
      search_keyword: String, // 用户搜索的关键词
      click_category_id: Long, // 某一个商品品类的 ID
      click_product_id: Long, // 某一个商品的 ID
      order_category_ids: String, // 一次订单中所有品类的 ID 集合
      order_product_ids: String, // 一次订单中所有商品的 ID 集合
      pay_category_ids: String, // 一次支付中所有品类的 ID 集合
      pay_product_ids: String, // 一次支付中所有商品的 ID 集合
      city_id: Long //城市 id
  )
}

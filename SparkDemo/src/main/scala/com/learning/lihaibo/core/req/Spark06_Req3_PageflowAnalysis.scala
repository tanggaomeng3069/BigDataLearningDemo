package com.learning.lihaibo.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 9:31
  * Describe:
  */
object Spark06_Req3_PageflowAnalysis {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // TODO 1.读取原始日志数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

        val actionRDD: RDD[UserVisitAction] = dataRDD.map(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                UserVisitAction(
                    datas(0),
                    datas(1).toLong,
                    datas(2),
                    datas(3).toLong,
                    datas(4),
                    datas(5),
                    datas(6).toLong,
                    datas(7).toLong,
                    datas(8),
                    datas(9),
                    datas(10),
                    datas(11),
                    datas(12).toLong
                )
            }
        )

        // 缓存
        actionRDD.cache()

        // TODO 对指定的页面连续跳转进行统计
        //  1-2，2-3，3-4，4-5，5-6，6-7
        val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)
        val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)

        // TODO 计算分母
        val pageidToCountMap: Map[Long, Long] = actionRDD.filter(
            (action: UserVisitAction) => {
                ids.init.contains(action.page_id)
            }
        ).map(
            (action: UserVisitAction) => {
                (action.page_id, 1L)
            }
        ).reduceByKey((_: Long) + (_: Long)).collect().toMap

        // TODO 计算分子
        //  根据session进行分组
        val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy((_: UserVisitAction).session_id)

        // TODO 分组后，根据访问时间进行排序（升序）
        val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
            (iter: Iterable[UserVisitAction]) => {
                val sortList: List[UserVisitAction] = iter.toList.sortBy((_: UserVisitAction).action_time)

                // 【1，2，3，4】
                // 【1，2】，【2，3】，【3，4】
                // 【1-2，2-3，3-4】
                // Sliding : 滑窗
                // 【1，2，3，4】
                // 【2，3，4】
                // zip : 拉链
                val flowIds: List[Long] = sortList.map((_: UserVisitAction).page_id)
                val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

                // 将不合法的页面跳转进行过滤
                pageflowIds.filter(
                    (t: (Long, Long)) => {
                        okflowIds.contains(t)
                    }
                ).map(
                    (t: (Long, Long)) => {
                        (t, 1)
                    }
                )
            }
        )

        // ((1,2),1)
        val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map((_: (String, List[((Long, Long), Int)]))._2).flatMap((list: List[((Long, Long), Int)]) => list)
        // ((1,2),1) => ((1,2),sum)
        val reduceRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey((_: Int) + (_: Int))

        // TODO 计算单跳转换率
        //  分子除以分母
        reduceRDD.foreach {
            case ((pageid1, pageid2), sum) => {
                val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)

                println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为: " + (sum.toDouble / lon))
            }
        }

        sc.stop()
    }

    // TODO 用户访问动作列表
    case class UserVisitAction(
                                date: String, //用户点击行为的日期
                                user_id: Long, //用户的ID
                                session_id: String, //Session的ID
                                page_id: Long, //某个页面的ID
                                action_time: String, //动作的时间点
                                search_keyword: String, //用户搜索的关键词
                                click_category_id: Long, //某一个商品品类的ID
                                click_product_id: Long, //某一个商品的ID
                                order_category_ids: String, //一次订单中所有品类的ID集合
                                order_product_ids: String, //一次订单中所有商品的ID集合
                                pay_category_ids: String, //一次支付中所有品类的ID集合
                                pay_product_ids: String, //一次支付中所有商品的ID集合
                                city_id: Long //城市 id
                              )

}

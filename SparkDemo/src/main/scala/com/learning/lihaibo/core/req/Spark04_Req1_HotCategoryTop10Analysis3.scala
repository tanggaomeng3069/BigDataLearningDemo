package com.learning.lihaibo.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author: tanggaomeng
  * Date: 2021/1/13 9:31
  * Describe:
  */
object Spark04_Req1_HotCategoryTop10Analysis3 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // TODO 1.读取原始日志数据
        val dataRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

        // 使用累加器，并注册
        val acc = new HotCategoryAccumulator
        sc.register(acc)

        // TODO 2.将数据转换结构
        dataRDD.foreach(
            (action: String) => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    acc.add((datas(6), "click"))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids: Array[String] = datas(8).split(",")
                    ids.foreach(
                        (id: String) => {
                            acc.add((id, "order"))
                        }
                    )
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids: Array[String] = datas(10).split(",")
                    ids.foreach(
                        (id: String) => {
                            acc.add((id, "pay"))
                        }
                    )
                }
            }
        )

        val accValue: mutable.Map[String, HotCategory] = acc.value
        val categories: mutable.Iterable[HotCategory] = accValue.map((_: (String, HotCategory))._2)

        val sort: List[HotCategory] = categories.toList.sortWith(
            (left: HotCategory, right: HotCategory) => {
                if (left.clickCnt > right.clickCnt) {
                    true
                } else if (left.clickCnt == right.clickCnt) {
                    if (left.orderCnt > right.orderCnt) {
                        true
                    } else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        )

        // TODO 5.将结果采集到控制台打印出来
        sort.foreach(println)

        sc.stop()
    }

    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

    /**
      * 自定义累加器
      * 1.继承AccumulatorV2，定义泛型
      * IN: (品类ID, 行为类型)
      * OUT: mutable.Map[String, HotCategory]
      * 2.重写方法（6）
      */
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

        // TODO 声明存储空集合
        private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

        // TODO 判断累加器是否初始化
        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        // TODO 复制累加器
        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
            new HotCategoryAccumulator()
        }

        // TODO 重置累加器
        override def reset(): Unit = {
            hcMap.clear()
        }

        // TODO 向累加器中增加值
        override def add(v: (String, String)): Unit = {
            val cid: String = v._1
            val actionType: String = v._2
            val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
            if (actionType == "click") {
                category.clickCnt += 1
            } else if (actionType == "order") {
                category.orderCnt += 1
            } else if (actionType == "pay") {
                category.payCnt += 1
            }
            hcMap.update(cid, category)

        }

        // TODO 合并累加器
        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            val map1: mutable.Map[String, HotCategory] = this.hcMap
            val map2: mutable.Map[String, HotCategory] = other.value

            map2.foreach {
                case (cid, hc) => {
                    val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
                    category.clickCnt += hc.clickCnt
                    category.orderCnt += hc.orderCnt
                    category.payCnt += hc.payCnt
                    map1.update(cid, category)
                }
            }
        }

        // TODO 返回累加器的值
        override def value: mutable.Map[String, HotCategory] = {
            hcMap
        }
    }

}

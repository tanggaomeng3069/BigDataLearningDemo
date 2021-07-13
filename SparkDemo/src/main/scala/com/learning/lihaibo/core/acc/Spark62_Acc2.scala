package com.learning.lihaibo.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/12 15:45
  * Describe:
  */
object Spark62_Acc2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // TODO 分布式共享只写变量
        //  所谓的累加器，一般的作用为累加（数值增加，数据的累加）数据
        //  1. 将累加器变量注册到spark中；
        //  2. 执行计算时，spark会将累加器发送到Executor端执行计算；
        //  3. 计算完毕后，Executor会将累加器的计算结果返回到Driver端；
        //  4. Driver端获取到多个累加器的结果，然后两两合并，最后得到累加器的执行结果；

        // TODO 声明累加器变量
        val sum: LongAccumulator = sc.longAccumulator("sum")
        //sc.doubleAccumulator()
        //sc.collectionAccumulator()

        dataRDD.foreach((num: Int) => {
            // TODO 使用累加器
            sum.add(num)
        })

        // TODO 获取累加器的结果
        println("结果为 = " + sum.value)


        sc.stop()

    }

}

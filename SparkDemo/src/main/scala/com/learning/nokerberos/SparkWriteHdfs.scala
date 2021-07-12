package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteHdfs {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        // 创建和配置SparkConf，如果是Yarn-Cluster模式提交任务，需要去掉 .setMaster("local[*]")
        val sparkConf: SparkConf = new SparkConf()
            .setAppName("SparkWriteHdfs")
            .setMaster("local[*]")
        // 建立连接
        val sc = new SparkContext(sparkConf)
        // 生成数据
        val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

        // 保存数据
        dataRDD.saveAsTextFile(noKerberosConfigLoader.getString("output.path"))
        println("保存数据成功！！！")
        sc.stop()


    }

}

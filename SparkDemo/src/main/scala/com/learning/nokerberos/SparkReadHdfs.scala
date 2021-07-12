package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadHdfs {

    def main(args: Array[String]): Unit = {

        // 创建和配置SparkConf，如果是Yarn-Cluster模式提交任务，需要去掉 .setMaster("local[*]")
        val sparkConf: SparkConf = new SparkConf()
            .setAppName("SparkWriteHdfs")
            .setMaster("local[*]")
        // 建立连接
        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[String] = sc.textFile(noKerberosConfigLoader.getString("input.path"))

        println(dataRDD.collect().mkString(","))

        sc.stop()
    }

}

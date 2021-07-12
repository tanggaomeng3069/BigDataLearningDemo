package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark读取HDFS中的文件，wordcount
 */
object ReadHDFSWordCount {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        // 设置kerberos认证
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"))

        val configuration = new Configuration()
        configuration.set("hadoop.security.authentication", "Kerberos")
        try {
            UserGroupInformation.setConfiguration(configuration)
            UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
            println("Kerberos认证成功: " + UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => println("Kerberos认证失败。。。")
        }

        // 如果是Yarn-Cluster模式提交任务，需要注释掉 .master("local[*]")
        val spark = SparkSession
            .builder()
            .appName("ReadHDFSWordCount")
            .master("local[*]")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取数据
        val fileRDD: RDD[String] = sc.textFile(KerberosConfigLoader.getString("input.path"))
        // 扁平化
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
        // 结构转换
        val mapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
        // 分词分组聚合
        val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        // 结果采集
        val wordCountArray: Array[(String, Int)] = wordToSumRDD.collect()
        // 打印控制台
        println(wordCountArray.mkString(","))

        // 释放连接
        sc.stop()

    }

}

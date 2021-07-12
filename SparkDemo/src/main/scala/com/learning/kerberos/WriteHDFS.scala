package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WriteHDFS {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
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
            .appName("WriteHDFS")
            .master("local[*]")
            .getOrCreate()
        val sc = spark.sparkContext

        // 生成数据
        val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
        // 保存数据
        dataRDD.saveAsTextFile(KerberosConfigLoader.getString("output.path"))
        println("保存数据成功！！！")

        sc.stop()
    }

}

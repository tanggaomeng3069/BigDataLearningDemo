package com.learning.kerberos

import java.io.IOException
import java.security.PrivilegedAction

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHiveWriteHDFS {

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

        // 如果是Yarn-Cluster模式提交任务，需要去掉 .setMaster("local[*]")
        val spark: SparkSession = SparkSession
            .builder()
            .appName("ReadHiveWriteHDFS")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        val dataFrame: DataFrame = spark.sql("select active,name,version,os,Core,mail from " + KerberosConfigLoader.getString("hive.table.name") + " limit 10")
        dataFrame.show()
        println("Query Hive Succeeded !^^!")

        // 保存到HDFS上
        dataFrame.rdd.saveAsTextFile(KerberosConfigLoader.getString("output.path"))

        println("Write to HDFS Succeeded !^^!")

        spark.stop()

    }
}

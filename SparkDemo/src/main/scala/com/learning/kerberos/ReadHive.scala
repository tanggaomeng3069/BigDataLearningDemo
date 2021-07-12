package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHive {

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
            .appName("ReadHive")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        val dataFrame: DataFrame = spark.sql("select * from " + KerberosConfigLoader.getString("hive.table.name") + " limit 10")
        dataFrame.show()

        println("Query Hive Succeeded !^^!")

        spark.stop()

    }

}

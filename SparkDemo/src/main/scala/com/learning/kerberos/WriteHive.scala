package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

object WriteHive {

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
            .appName("WriteHive")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        // 示例数据
        val dataRDD: RDD[String] = spark.sparkContext.makeRDD(List("1,Ambari,V2.7.3,CentOS,7.4.1708,bigdata@ambari.com"))

        // 样例类
        case class Person(name: String, version: String, mail: String)
        val schema: StructType = types.StructType(
            Seq(
                StructField("active", StringType, nullable = true),
                StructField("name", StringType, nullable = true),
                StructField("version", StringType, nullable = true),
                StructField("os", StringType, nullable = true),
                StructField("Core", StringType, nullable = true),
                StructField("mail", StringType, nullable = true)
            )
        )
        val rowRDD: RDD[Row] = dataRDD.map(x => {
            val value: Array[String] = x.split(",")
            Row(value(0), value(1), value(2), value(3), value(4), value(5))
        })

        val frame: DataFrame = spark.createDataFrame(rowRDD, schema)
        frame.createOrReplaceGlobalTempView("test")
        spark.sql("use " + KerberosConfigLoader.getString("hive.db.name"))
        frame.write.mode(SaveMode.Append).format("hive").saveAsTable(KerberosConfigLoader.getString("hive.table.name"))
        println("Insert Hive Successfully !^^!")

        spark.stop()

    }

}

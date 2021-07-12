package com.learning.kerberos

import java.io.IOException
import java.util

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object ReadHBase {

    def main(args: Array[String]): Unit = {
        // 设置日志级别
        Logger.getRootLogger.setLevel(Level.WARN)

        val javaSecurityKrb5Conf = KerberosConfigLoader.getString("java.security.krb5.conf")
        val javaSecurityAuthLoginConfig = KerberosConfigLoader.getString("java.security.auth.login.config")

        // 设置kerberos认证，本地模式和集群模式需要修改配置，设置正确
        System.setProperty("java.security.krb5.conf", javaSecurityKrb5Conf)
        System.setProperty("java.security.auth.login.config", javaSecurityAuthLoginConfig)

        // Kerberos认证
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", KerberosConfigLoader.getString("hbase.zookeeper.list"))
        conf.set("hbase.zookeeper.property.clientPort", KerberosConfigLoader.getString("hbase.zookeeper.port"))
        conf.set("hadoop.security.authentication", "Kerberos")
        conf.set("hbase.security.authentication", "Kerberos")
        conf.set("hbase.master.kerberos.principal", KerberosConfigLoader.getString("hbase.master.kerberos.principal"))
        conf.set("hbase.regionserver.kerberos.principal", KerberosConfigLoader.getString("hbase.regionserver.kerberos.principal"))
        conf.set("zookeeper.znode.parent", KerberosConfigLoader.getString("zookeeper.znode.parent"))
        conf.set("hbase.client.retries.number", KerberosConfigLoader.getString("hbase.client.retries.number")) // 设置重试次数
        try {
            UserGroupInformation.setConfiguration(conf)
            UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
            println("Kerberos认证成功: " + UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => println("Kerberos认证失败。。。")
        }

        val spark: SparkSession = SparkSession
            .builder()
            .appName("ReadHBase")
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=" + javaSecurityAuthLoginConfig)
            .master("local[*]")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext

        val tableName: String = KerberosConfigLoader.getString("hbase.table.name")
        val hbaseContext = new HBaseContext(sc, conf)
        val scan = new Scan()
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
        
        val result = hbaseRDD.map(x => {
            val it: util.Iterator[Cell] = x._2.listCells().iterator()
            var action = ""
            var name = ""
            var version = ""
            var os = ""
            var Core = ""
            var mail = ""
            while (it.hasNext) {
                val cell: Cell = it.next()
                val value: String = Bytes.toString(CellUtil.cloneQualifier(cell))
                if (value.equals("action")) {
                    action = Bytes.toString(CellUtil.cloneValue(cell))
                } else if (value.equals("name")) {
                    name = Bytes.toString(CellUtil.cloneValue(cell))
                } else if (value.equals("version")) {
                    version = Bytes.toString(CellUtil.cloneValue(cell))
                } else if (value.equals("os")) {
                    os = Bytes.toString(CellUtil.cloneValue(cell))
                } else if (value.equals("Core")) {
                    Core = Bytes.toString(CellUtil.cloneValue(cell))
                } else if (value.equals("mail")) {
                    mail = Bytes.toString(CellUtil.cloneValue(cell))
                }
            }
            (action, name, version, os, Core, mail)
        })

        println(result.collect().mkString("-"))

        sc.stop()

    }

}

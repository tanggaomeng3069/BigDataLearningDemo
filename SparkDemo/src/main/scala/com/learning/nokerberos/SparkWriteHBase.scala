package com.learning.nokerberos

import java.util.UUID

import com.learning.utils.noKerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

object SparkWriteHBase {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .appName("SparkOnHBase")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext

        // 示例数据
        val dataRDD: RDD[(String, String)] = sc.makeRDD(List(
            ("action", "1"), ("name", "Insight"), ("version", "V4.6.5"), ("os", "CentOS"), ("Core", "7.4.1708"), ("mail", "bigdata@inspur.com")
        ))

        dataRDD.foreachPartition(partition => {
            val conf: Configuration = HBaseConfiguration.create()
            conf.set("hbase.zookeeper.quorum", noKerberosConfigLoader.getString("hbase.zookeeper.list"))
            conf.set("hbase.zookeeper.property.clientPort", noKerberosConfigLoader.getString("hbase.zookeeper.port"))
            conf.set("zookeeper.znode.parent", noKerberosConfigLoader.getString("zookeeper.znode.parent"))
            conf.set("hbase.client.retries.number", noKerberosConfigLoader.getString("hbase.client.retries.number"))
            val hbaseConnection: Connection = ConnectionFactory.createConnection(conf)
            partition.foreach(line => {
                val tableName: TableName = TableName.valueOf(noKerberosConfigLoader.getString("hbase.table.name"))
                val table: Table = hbaseConnection.getTable(tableName)
                val uuid: String = UUID.randomUUID().toString

                val put = new Put(Bytes.toBytes(uuid))
                val tableColumnFamily: String = noKerberosConfigLoader.getString("hbase.table.column.family")
                put.addColumn(Bytes.toBytes(tableColumnFamily), Bytes.toBytes(line._1.toString), Bytes.toBytes(line._2.toString))

                // 将数据写入HBase，若出错关闭table
                Try(table.put(put)).getOrElse(table.close())
                // 分区写入HBase后关闭连接
                table.close()
            })
            hbaseConnection.close()
        })


        sc.stop()
    }

}

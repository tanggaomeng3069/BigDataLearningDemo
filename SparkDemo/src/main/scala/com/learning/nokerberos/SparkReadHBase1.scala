package com.learning.nokerberos

import java.util

import com.learning.utils.noKerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkReadHBase1 {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .appName("SparkOnHBase")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext

        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", noKerberosConfigLoader.getString("hbase.zookeeper.list"))
        conf.set("hbase.zookeeper.property.clientPort", noKerberosConfigLoader.getString("hbase.zookeeper.port"))
        conf.set("zookeeper.znode.parent", noKerberosConfigLoader.getString("zookeeper.znode.parent"))
        conf.set("hbase.client.retries.number", noKerberosConfigLoader.getString("hbase.client.retries.number"))
        val hbaseConnection: Connection = ConnectionFactory.createConnection(conf)

        val tableName: String = noKerberosConfigLoader.getString("hbase.table.name")
        hbaseConnection.getConfiguration.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

        val scan: Scan = new Scan()
        scan.addFamily(Bytes.toBytes(noKerberosConfigLoader.getString("hbase.table.column.family")))

        // 方法1：
        val tableName1: TableName = TableName.valueOf(tableName)
        val table: Table = hbaseConnection.getTable(tableName1)
        val scanner: ResultScanner = table.getScanner(scan);
        val value: util.Iterator[Result] = scanner.iterator()
        while (value.hasNext) {
            val result: Result = value.next()
            for (cell <- result.rawCells) {
                System.out.println("rowkey: " + Bytes.toString(CellUtil.cloneRow(cell)) + ", Column-family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + ", Column: " + Bytes.toString(CellUtil.cloneQualifier(cell)) + ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)))
            }
        }

        sc.stop()
    }

}

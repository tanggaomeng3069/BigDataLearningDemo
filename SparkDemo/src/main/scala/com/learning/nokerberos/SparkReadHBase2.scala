package com.learning.nokerberos

import java.util.Base64

import com.learning.utils.noKerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkReadHBase2 {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .appName("SparkReadHBase2")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext

        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", noKerberosConfigLoader.getString("hbase.zookeeper.list"))
        conf.set("hbase.zookeeper.property.clientPort", noKerberosConfigLoader.getString("hbase.zookeeper.port"))
        conf.set("zookeeper.znode.parent", noKerberosConfigLoader.getString("zookeeper.znode.parent"))
        conf.set("hbase.client.retries.number", noKerberosConfigLoader.getString("hbase.client.retries.number"))


        val tableName: String = noKerberosConfigLoader.getString("hbase.table.name")
        conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)
        val scan: Scan = new Scan()
        scan.addFamily(Bytes.toBytes(noKerberosConfigLoader.getString("hbase.table.column.family")))

        // 方法2：
        val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
        val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray))
        conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)

        // 读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        // 方法简洁
        hbaseRDD.map(x => x._2).foreach(line => {
            for (cell <- line.rawCells) {
                System.out.println("rowkey: " + Bytes.toString(CellUtil.cloneRow(cell)) + ", Column-family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + ", Column: " + Bytes.toString(CellUtil.cloneQualifier(cell)) + ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)))
            }
        })


        // 方法比较笨重
//        val resultRDD: RDD[(String, String, String, String, String, String)] = hbaseRDD.map(x => {
//            val it: util.Iterator[Cell] = x._2.listCells().iterator()
//            var name = ""
//            var version = ""
//            var mail = ""
//            var action = ""
//            var os = ""
//            var Core = ""
//            while (it.hasNext) {
//                val cell: Cell = it.next()
//                val value: String = Bytes.toString(CellUtil.cloneQualifier(cell))
//                if (value.equals("name")) {
//                    name = Bytes.toString(CellUtil.cloneValue(cell))
//                } else if (value.equals("version")) {
//                    version = Bytes.toString(CellUtil.cloneValue(cell))
//                } else if (value.equals("mail")) {
//                    mail = Bytes.toString(CellUtil.cloneValue(cell))
//                } else if (value.equals("action")) {
//                    action = Bytes.toString(CellUtil.cloneValue(cell))
//                } else if (value.equals("os")) {
//                    os = Bytes.toString(CellUtil.cloneValue(cell))
//                } else if (value.equals("Core")) {
//                    Core = Bytes.toString(CellUtil.cloneValue(cell))
//                }
//            }
//            (action, name, version, mail, os, Core)
//        })
//        println(resultRDD.collect().mkString(", "))


        sc.stop()
    }

}

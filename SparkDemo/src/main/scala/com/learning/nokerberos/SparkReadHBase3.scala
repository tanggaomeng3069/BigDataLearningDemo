package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkReadHBase3 {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .appName("SparkReadHBase3")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext

        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", noKerberosConfigLoader.getString("hbase.zookeeper.list"))
        conf.set("hbase.zookeeper.property.clientPort", noKerberosConfigLoader.getString("hbase.zookeeper.port"))
        conf.set("zookeeper.znode.parent", noKerberosConfigLoader.getString("zookeeper.znode.parent"))
        conf.set("hbase.client.retries.number", noKerberosConfigLoader.getString("hbase.client.retries.number"))

        val hbaseContext = new HBaseContext(sc, conf)
        val scan = new Scan()

        val tableName: String = noKerberosConfigLoader.getString("hbase.table.name")
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
//        getRdd.foreach(v => println(Bytes.toString(v._1.get())))
//        println("Length: " + getRdd.map(r => r._1.copyBytes()).collect().length)

        hbaseRDD.map(x => x._2).foreach(line => {
            for (cell <- line.rawCells) {
                System.out.println("rowkey: " + Bytes.toString(CellUtil.cloneRow(cell)) + ", Column-family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + ", Column: " + Bytes.toString(CellUtil.cloneQualifier(cell)) + ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)))
            }
        })


        sc.stop()
    }

}

package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.spark.sql.SparkSession
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration


object Spark2JanusEmbed {

  val janusConf = new CommonsConfiguration
  janusConf.set("storage.backend", "hbase")
  janusConf.set("storage.hbase.table", noKerberosConfigLoader.getString("storage.hbase.table"))
  janusConf.set("storage.hostname", noKerberosConfigLoader.getString("storage.hostname"))
  janusConf.set("storage.hbase.ext.zookeeper.znode.parent", noKerberosConfigLoader.getString("storage.zk.parent"))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("janus-write-demo")
      .getOrCreate()

    //示例数据位置：src/main/resources/data/test-data.txt，自行上传到HDFS上
    val rdd = spark.read.csv(noKerberosConfigLoader.getString("janus.test.data.path"))
      .rdd.map(x => {
      (x.getString(0), x.getString(1), x.getString(2))
    })


    rdd.foreachPartition { x => {

      val graph = JanusGraphFactory.open(janusConf)

      val g = graph.traversal()

      var sum = 0L
      var counts = 0L
      try {
        x.foreach(y => {
          g.addV(y._1).property("prop1", y._2).property("prop2", y._3).next()
          counts += 1
          if (counts == 1) {
            g.tx().commit()
            sum = sum + counts
            println("############################################################")
            println("#########################" + sum + "#############################")
            println("############################################################")
            counts = 0
          }
        })
      } finally {
        g.tx().commit()
      }
      println("--------over!-------")
      val count = graph.traversal().V().count().next()
      println("一共写入数据条数：" + count)
      g.tx.close()
      graph.close()
    }
    }

    spark.close()
  }
}

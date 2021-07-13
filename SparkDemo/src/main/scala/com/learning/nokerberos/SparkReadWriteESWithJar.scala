package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * 提交命令参考11
  */
object SparkReadWriteESWithJar {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark-ES-Demo")
      .config("es.nodes", noKerberosConfigLoader.getString("es.nodes"))
      .config("es.port", noKerberosConfigLoader.getString("es.port"))
      .config("es.nodes.wan.only", noKerberosConfigLoader.getString("es.nodes.wan.only"))
      .getOrCreate()

    val sqc = spark.sqlContext

    /**
      * 先读一下index数据，业务方酌情修改resource和query
      */
    val esDf = EsSparkSQL.esDF(sqc, noKerberosConfigLoader.getString("es.query.index"),
      noKerberosConfigLoader.getString("es.query"))
    esDf.show(false)

    /**
      * 两种写ES的方法，不同jar包提供
      */
    //第一种使用elasticsearch-spark-20_2.11 jar
//    val oneDf = spark.sparkContext.parallelize(Seq(
//      List("new1", "today1", "red"),
//      List("new2", "today2", "blue"))
//    ).map(r => (r.head, r(1), r(2)))
//      .toDF("content", "publish_date", "status")
//
//    EsSparkSQL.saveToEs(oneDf, noKerberosConfigLoader.getString("es.query.index"))


    //第二种使用elasticsearch-hadoop jar
    val oneRdd = spark.sparkContext.makeRDD(Seq(
      Map("content" -> "new3", "publish_date" -> "today3", "status" -> "no"),
      Map("content" -> "new4", "publish_date" -> "today4", "status" -> "bug")
    ))
//    oneRdd.saveToEs(noKerberosConfigLoader.getString("es.query.index"))


    //再读ES,查看写入的数据是否成功
    val esDfNew = EsSparkSQL.esDF(sqc, noKerberosConfigLoader.getString("es.query.index"),
      noKerberosConfigLoader.getString("es.query"))
    esDfNew.show(false)

  }

}

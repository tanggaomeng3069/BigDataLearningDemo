package com.learning.kerberos

import com.learning.utils.KerberosConfigLoader
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * ###准备工作！
  *  选取一个节点生成PKCS#12 (.p12) 认证证书：
  *  ./bin/elasticsearch-certutil cert --ca config/elastic-stack-ca.p12 --ip 192.168.100.120 --days 3650 --name test1
  *  生成证书时需要记住密码
  *  这个证书不需要每个节点都有，但必须通过--files提交-写绝对路径
  *  注意：其他引用证书(test1.p12)的地方写的相对路径！
  * ###
  * 如下：
  * spark-submit \
    --master yarn \
    --deploy-mode client  \
    --class com.insight.spark.security.SparkReadWriteESWithJar \
    --principal spark/test01@BIGDATA \
    --keytab /etc/security/keytabs/spark.service.keytab \
    --driver-java-options '-Djavax.net.ssl.trustStore=./test1.p12 -Djavax.net.ssl.trustStorePassword=admin123' \
    --conf 'spark.executor.extraJavaOptions=-Djavax.net.ssl.trustStore=./test1.p12 -Djavax.net.ssl.trustStorePassword=admin123' \
    --files /root/test1.p12 spark-1.0-SNAPSHOT.jar <es.user> <es.passwd>
  *
  */

object SparkReadWriteESWithJar {

  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
      .appName("Spark-ES-Demo")
      .config("es.nodes", KerberosConfigLoader.getString("es.nodes"))
      .config("es.port", KerberosConfigLoader.getString("es.port"))
      .config("es.nodes.wan.only", KerberosConfigLoader.getString("es.nodes.wan.only"))

    if(args.length==2){
      Logger.getLogger(getClass).info("用户覆写：spark-submit ...jar username password")
      sparkBuilder.config("es.net.http.auth.user", args(0))
        .config("es.net.http.auth.pass", args(1))
    } else {
      Logger.getLogger(getClass).info("默认读取：security.properties ES username/password")
      sparkBuilder.config("es.net.http.auth.user", KerberosConfigLoader.getString("es.net.http.auth.user"))
        .config("es.net.http.auth.pass",KerberosConfigLoader.getString("es.net.http.auth.pass"))
    }
    val spark = sparkBuilder.getOrCreate()

    val sqc = spark.sqlContext

    /**
      * 先读一下index数据，业务方酌情修改resource和query
      */
    val esDf = EsSparkSQL.esDF(sqc, KerberosConfigLoader.getString("es.query.index"),
      KerberosConfigLoader.getString("es.query"))
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

//    EsSparkSQL.saveToEs(oneDf, KerberosConfigLoader.getString("es.query.index"))


    //第二种使用elasticsearch-hadoop jar
    val oneRdd = spark.sparkContext.makeRDD(Seq(
      Map("content" -> "new3", "publish_date" -> "today3", "status" -> "no"),
      Map("content" -> "new4", "publish_date" -> "today4", "status" -> "bug")
    ))
//    oneRdd.saveToEs(KerberosConfigLoader.getString("es.query.index"))


    //再读ES,查看写入的数据是否成功
    val esDfNew = EsSparkSQL.esDF(sqc, KerberosConfigLoader.getString("es.query.index"),
      KerberosConfigLoader.getString("es.query"))
    esDfNew.show(false)

  }

}

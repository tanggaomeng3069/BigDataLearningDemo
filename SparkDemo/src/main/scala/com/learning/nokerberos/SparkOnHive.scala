package com.learning.nokerberos

import com.learning.utils.noKerberosConfigLoader
import org.apache.spark.sql.SparkSession

/**
 * hive-site.xml必须下载 /usr/hdp/3.0.1.0-187/spark2/conf/hive-site.xml文档到resource
 * hdfs://manager116.bigdata:8020/tmp/test.txt 必须hive用户有权限
 */
object SparkOnHive {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hive")

        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SparkWriteHive")
            .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
            .enableHiveSupport()
            .getOrCreate()

        spark.sql("use " + noKerberosConfigLoader.getString("hive.database.name"))
        spark.sql("show tables").show()
        spark.sql(noKerberosConfigLoader.getString("hive.create.table.sql"))
        spark.sql("show tables").show()

        spark.sql(noKerberosConfigLoader.getString("hive.load.data.into.table"))
        spark.sql(noKerberosConfigLoader.getString("hive.table.select")).show()

        spark.stop()

    }

}

package com.learning.lihaibo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 17:30
  * Describe:
  */
object SparkSQL10_Hive {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .enableHiveSupport()  // 必须启动Hive支持
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        // TODO 使用SparkSQL连接内置的Hive
        //  1.启动hive的支持
        //  2.增加对应的依赖关系（包含MySQL驱动）

//        spark.sql("show tables").show()
//        spark.sql("create table aa(id int)")
//        spark.sql("show tables").show()

//        spark.sql("load data local inpath 'input/id.txt' into table aa")
//        spark.sql("select * from aa").show()

        // TODO 如果连接外置的Hive，需要把Hive on Tez去掉
        //  目前还不知道如何在Ambari中去掉Hive on Tez 修改为 Hive on Spark
        spark.sql("show databases").show()


        spark.close()

    }

}

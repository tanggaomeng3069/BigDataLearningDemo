package com.learning.xiaoniuxuetang.sparksql

import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author: tanggaomeng
  * Date: 2020/8/30 15:07
  * Describe:
  */
object SQLTest2 {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("SQLTest2")
      .master("local[*]")
//      .withExtensions(extensions => {
//        // 自定义解析器
//        extensions.injectResolutionRule { session =>
//          ...
//        }
//        extensions.injectParser { (session, parser) =>
//          ...
//        }
//      })
      .getOrCreate()
//    sparkSession.sparkContext
//    sparkSession.sqlContext
//    SparkSession.builder().enableHiveSupport()


    // 导入隐式转换
    import sparkSession.implicits._

    // 指定从哪儿读数据，lazy
    // Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    // Dataset只有一列，默认这列叫value
    val words: Dataset[String] = sparkSession
      .read
      .textFile("hdfs://managerhd.bigdata:8020/wc")
      .flatMap((_: String)
        .split(" "))
//    sparkSession.sparkContext.textFile

    // 注册视图
    words.createTempView("v_wc")

    // 执行SQL（Transformation(lazy)）
    val result: DataFrame = sparkSession.sql("select value, count(*) counts from v_wc group by value order by counts desc")

    sparkSession.catalog.listTables().show(false)
    // 触发Action
    result.show()
    // 查看执行计划
    result.explain(true)

    sparkSession.stop()


  }

}

package com.learning.lihaibo.core.framework.service

import com.learning.lihaibo.core.framework.common.TService
import com.learning.lihaibo.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:18
  * Describe: 服务层
  */
class WordCountService extends TService {

    private val wordCountDao = new WordCountDao()

    // TODO 数据分析
    def dataAnalysis(): Array[(String, Int)] = {
        val dataRDD: RDD[String] = wordCountDao.readFile("input/word.txt")
        val wordsRDD: RDD[String] = dataRDD.flatMap((_: String).split(" "))
        val mapRDD: RDD[(String, Int)] = wordsRDD.map((word: String) => (word, 1))
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey((_: Int) + (_: Int))
        val result: Array[(String, Int)] = reduceRDD.collect()
        result
    }

}


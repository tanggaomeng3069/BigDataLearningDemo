package com.learning.lihaibo.core.framework.controller

import com.learning.lihaibo.core.framework.common.TController
import com.learning.lihaibo.core.framework.service.WordCountService

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:16
  * Describe: 控制层
  */
class WordCountController extends TController{

    private val wordCountService = new WordCountService()

    // TODO 调度
    def dispatch(): Unit = {

        // 执行业务操作
        val array: Array[(String, Int)] = wordCountService.dataAnalysis()
        array.foreach(println)
    }
}

package com.learning.lihaibo.core.framework.common

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:08
  * Describe: 三层架构中的：服务
  */
trait TService {

    def dataAnalysis(): Array[(String, Int)]

}

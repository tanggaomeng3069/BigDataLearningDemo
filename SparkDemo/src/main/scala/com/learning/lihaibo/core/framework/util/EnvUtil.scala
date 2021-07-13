package com.learning.lihaibo.core.framework.util

import org.apache.spark.SparkContext

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 14:48
  * Describe:
  */
object EnvUtil {

    // 线程共享资源
    private val scLocal = new ThreadLocal[SparkContext]()

    def put(sc: SparkContext): Unit = {
        scLocal.set(sc)
    }

    def take(): SparkContext = {
        scLocal.get()
    }

    def clear(): Unit = {
        scLocal.remove()
    }

}

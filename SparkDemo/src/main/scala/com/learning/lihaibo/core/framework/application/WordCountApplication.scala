package com.learning.lihaibo.core.framework.application

import com.learning.lihaibo.core.framework.common.TApplication
import com.learning.lihaibo.core.framework.controller.WordCountController

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:14
  * Describe:
  * 1.extends APP之后就用写main函数，就能执行伴生对象
  */
object WordCountApplication extends App with TApplication {

    // TODO 启动程序
    start(){
        val wordCountController = new WordCountController()
        wordCountController.dispatch()
    }

}

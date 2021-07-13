package com.learning.scalaHanShunPing.chapter01

/**
  * Author: tanggaomeng
  * Date: 2020/11/12 19:27
  * Describe:
  */
object Scala002_printDemo {

    def main(args: Array[String]): Unit = {
        var str1: String = "hello"
        var str2: String = "world!"
        println(str1 + " " + str2)

        var name: String = "tom"
        var age: Int = 10
        var sal: Float = 10.67f
        var height: Double = 180.15
        // 格式化输出
        printf("名字=%s 年龄=%d 薪水=%.2f 身高=%.3f", name, age, sal, height)

        // scala支持使用$输出内容，编译器回去解析$对应的变量
        println(s"\n个人信息如下：\n名字=$name \n年龄=$age \n薪水=$sal \n身高=$height")
        // 如果字符串中出现了类似${age + 10} 则表示 {} 是一个表达式
        println(s"\n个人信息如下2：\n名字=${name} \n年龄=${age + 10} \n薪水=${sal * 100} \n身高=${height}")


    }

}

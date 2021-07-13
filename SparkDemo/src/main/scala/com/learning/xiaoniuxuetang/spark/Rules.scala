package com.learning.xiaoniuxuetang.spark

import java.net.InetAddress

/**
  * Author: tanggaomeng
  * Date: 2020/8/27 16:07
  * Describe:
  */
//第一种方式，Rules有多个，从Driver发送到Executor中，被Executor中Task一一对应
//class Rules extends Serializable {
//
//  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)
//
//  //val hostname = InetAddress.getLocalHost.getHostName
//
//  //println(hostname + "@@@@@@@@@@@@@@@@")
//
//}

//第二种方式，Rules只有一个，从Driver发送到Executor中，被Executor多个Tassk使用
//object Rules extends Serializable {
//
//  val rulesMap: Map[String, Double] = Map("hadoop" -> 2.7, "spark" -> 2.2)
//
//  val hostname: String = InetAddress.getLocalHost.getHostName
//
//  println("%s@@@@@@@@@@@@@@@@！！！！".format(hostname))
//
//}


//第三种方式，希望Rules在EXecutor中被初始化（不走网络了，就不必实现序列化接口），Rules只有一个
object Rules {

  val rulesMap: Map[String, Double] = Map("hadoop" -> 2.7, "spark" -> 2.2)

  val hostname: String = InetAddress.getLocalHost.getHostName

  println("%s@@@@@@@@@@@@@@@@！！！！".format(hostname))

}

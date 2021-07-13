package com.learning.xiaoniuxuetang.sparkstreamingredis

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Author: tanggaomeng
  * Date: 2020/9/24 10:33
  * Describe: jedis创建redis连接池
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()
  // 最大连接数
  config.setMaxTotal(20)
  // 最大空闲连接数
  config.setMaxIdle(10)
  // 当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(true)
  // 10000代表超时时间（10秒）
  // 启动 redis-server redis.conf
  // 登录 redis-cli -h 10.180.210.232 -p 6379
  // 设置密码 config set requirepass 123
  // 登录后输入密码认证 AUTH 123
  val pool = new JedisPool(config, "10.180.210.232", 6379, 10000, "123")

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {

    val conn: Jedis = JedisConnectionPool.getConnection()

//    conn.set("income", "1000")
//    val r1: String = conn.get("income")
//    println(r1)
//
//    conn.incrBy("income", -50)
//    val r2: String = conn.get("income")
//    println(r2)
//
//    conn.close()

    val str: util.Set[String] = conn.keys("*")
    import scala.collection.JavaConversions._
    for (p <- str){
      println(p + " : " + conn.get(p))
    }
  }

}

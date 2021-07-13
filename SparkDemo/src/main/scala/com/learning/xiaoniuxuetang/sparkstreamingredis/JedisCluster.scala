package com.learning.xiaoniuxuetang.sparkstreamingredis

import java.util
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig}


/**
  * Author: tanggaomeng
  * Date: 2020/9/24 11:11
  * Describe: jedis连接redis集群
  */
object JedisCluster {

  val config = new JedisPoolConfig()
  // 最大连接数
  config.setMaxTotal(20)
  // 最大空闲连接数
  config.setMaxIdle(10)
  // 当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(true)

  // 集群节点
  def redisConnect(): JedisCluster = {
    val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort("10.180.210.232", 7001))
    jedisClusterNodes.add(new HostAndPort("10.180.210.232", 7002))
    jedisClusterNodes.add(new HostAndPort("10.180.210.232", 7003))
    jedisClusterNodes.add(new HostAndPort("10.180.210.232", 7004))
    jedisClusterNodes.add(new HostAndPort("10.180.210.232", 7005))
    jedisClusterNodes.add(new HostAndPort("10.180.210.232", 7006))
    lazy val jedisCluster = new JedisCluster(jedisClusterNodes, config)
    jedisCluster
  }

  def main(args: Array[String]): Unit = {
    val jedisCluster: JedisCluster = redisConnect()
    val clusterNodes: util.Map[String, JedisPool] = jedisCluster.getClusterNodes
    val it: util.Iterator[util.Map.Entry[String, JedisPool]] = clusterNodes.entrySet().iterator()

    while (it.hasNext) {
      val entry: util.Map.Entry[String, JedisPool] = it.next()
      val jedis: Jedis = entry.getValue.getResource
      if (!jedis.info("replication").contains("role:slave")) {
        val keys: util.Set[String] = jedis.keys("*")
        if (keys.size() > 0) {
          val it2: util.Iterator[String] = keys.iterator()
          while (it2.hasNext) {
            val key: String = it2.next()
            val value: String = jedis.get(key)
            println(key + " : " + value)
          }

        }

      }

    }

  }

}

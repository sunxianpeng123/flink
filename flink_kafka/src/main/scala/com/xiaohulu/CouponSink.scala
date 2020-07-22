//自定义sink
package com.xiaohulu

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import java.util.Properties
import java.io.FileInputStream
import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, Jedis, JedisPool, JedisPoolConfig}
class CouponSink extends RichSinkFunction[(String,Int)]{

  private val jedisClusterNodes = new util.HashSet[HostAndPort]()
  private val IP = "113.107.166.14"
  private val redis_passwd = "root"
  private val ports = 46379
  var cluster:Jedis = _
  var pool:JedisPool=_
  override def invoke(value:(String,Int)) {
    cluster =pool.getResource
    cluster.select(9)
    val blackName="www_blocklist_"
    System.out.println(value)
    cluster.setex(blackName+value._1,value._2,"over 1200 in "+value._2/60+" mins")
  }

  override def open( parameters:Configuration) {
    System.out.println("open")
    if (cluster == null){

      val jedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(60)
      jedisPoolConfig.setMaxIdle(60)
      jedisPoolConfig.setMinIdle(60)
      jedisPoolConfig.setNumTestsPerEvictionRun(1024)
      jedisPoolConfig.setTimeBetweenEvictionRunsMillis(30000)
      jedisPoolConfig.setMinEvictableIdleTimeMillis(1800000)
      jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(1800000)
      jedisPoolConfig.setMaxWaitMillis(500)
      jedisPoolConfig.setTestOnBorrow(false)
      jedisPoolConfig.setTestWhileIdle(true)
      jedisPoolConfig.setBlockWhenExhausted(false)
      pool = new JedisPool(jedisPoolConfig, IP,ports)


    }

  }

  override def close() {
    System.out.println("close");
   // cluster.close()

  }
}
package com.rox.dmp.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object JedisPools {

  lazy val config: Config = ConfigFactory.load()
  lazy val host = config.getString("redis.host")
  lazy val port = config.getInt("redis.port")


  lazy val pconfig = new GenericObjectPoolConfig()
  pconfig.setMaxTotal(1000)

  val jedisPool = new JedisPool(pconfig,host,port,3000,null,8)  //最后3个: 超时时间, 密码, 数据库号

  def getConn(): Jedis = jedisPool.getResource

}

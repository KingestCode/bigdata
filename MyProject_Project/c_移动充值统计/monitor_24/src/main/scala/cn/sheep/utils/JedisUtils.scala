package cn.sheep.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object JedisUtils {


    val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "ND-04")

    def getJedisClient(): Jedis = {
        jedisPool.getResource
    }

}

package com.rox.redis_jedis.set_test;

import java.util.Set;

import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * 测试 SetAPI
 * 是否属于
 * 交集
 * 并集
 * 差集
 */
public class SetTest {

    @Test
    public void testSet() {
        Jedis jedis = new Jedis("cs1", 6379);

        jedis.sadd("bigdata:hadoop", "hdfs","maprduce","yarn","zookeeper");
        jedis.sadd("bigdata:spark", "flume","kafka","redis","zookeeper");

        // 判断一个成员是否属于指定的集合 sismember
        Boolean isornot = jedis.sismember("bigdata:hadoop", "zookeeper");
        System.out.println(isornot);
        System.out.println("----------------华丽的分割线--------------------");

        // 求两个集合的差集    sdiff
        Set<String> diff = jedis.sdiff("bigdata:hadoop","bigdata:spark");
        for(String mb:diff){
            System.out.println(mb);
        }
        System.out.println("----------------华丽的分割线--------------------");

        // 求两个集合的并集    sunion
        Set<String> union = jedis.sunion("bigdata:hadoop","bigdata:spark");
        for(String mb:union){
            System.out.println(mb);
        }
        System.out.println("----------------华丽的分割线--------------------");

        // 求两个集合的交集     sinter
        Set<String> intersect = jedis.sinter("bigdata:hadoop","bigdata:spark");
        //打印结果
        for(String mb:intersect){
            System.out.println(mb);
        }
    }

}

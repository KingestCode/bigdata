package com.rox.redis_jedis.zset;

import java.util.Random;

import redis.clients.jedis.Jedis;

/**
 * 测试SortedSet有序集合
 */
public class LOLBoxPlayer {

    public static void main(String[] args) throws Exception {

        Jedis jedis = new Jedis("cs1", 6379);

        Random random = new Random();
        String[] heros = {"易大师","德邦","剑姬","盖伦","阿卡丽","金克斯","提莫","猴子","亚索"};
        while(true){

            int index = random.nextInt(heros.length);
            //选择一个英雄
            String hero = heros[index];

            //开始玩游戏
            Thread.sleep(1000);

            //给集合中的该英雄的出场次数加1
            //第一次添加的时候，集合不存在，zincrby方法会创建
            // 将 member: hero 对应的 key:hero:ccl:phb 的 socre +1
            jedis.zincrby("hero:ccl:phb", 1, hero);

            System.out.println(hero+" 出场了.......");
        }
    }
}

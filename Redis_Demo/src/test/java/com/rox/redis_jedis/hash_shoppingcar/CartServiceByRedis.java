package com.rox.redis_jedis.hash_shoppingcar;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

/**
 * 测试Redis的Hash
 */
public class CartServiceByRedis {

    private Jedis jedis = null;
    private static final String CART_PRIFIX = "cart:";

    @Before
    public void init() {
        jedis = new Jedis("cs1", 6379);
    }

    /**
     * 添加商品到购物车
     */
    @Test
    public void testAddItemToCart() {
        jedis.hset(CART_PRIFIX + "huangbo", "小米max2", "2");
        jedis.hset(CART_PRIFIX + "xuzheng", "绿萝", "100");
        jedis.hset(CART_PRIFIX + "wangbaoqiang", "茶杯", "6");
        jedis.hset(CART_PRIFIX + "huangbo", "书", "10");
        jedis.hset(CART_PRIFIX + "wangbaoqiang", "瓷碗", "8");
        jedis.close();
    }

    /**
     * 查询购物车信息
     */
    @Test
    public void testGetCartInfo() {
        Map<String, String> cart = jedis.hgetAll(CART_PRIFIX + "wangbaoqiang");
        Set<Entry<String, String>> entrySet = cart.entrySet();
            for (Entry<String, String> ent : entrySet) {
                System.out.println(ent.getKey() + ":" + ent.getValue());
        }
        jedis.close();
    }

    /**
     * 更改购物车
     */
    @Test
    public void editCart() {
        //给蜡烛商品项的数量加1
        jedis.hincrBy(CART_PRIFIX + "wangbaoqiang", "瓷碗", 2);
        jedis.close();
    }

    /**
     * 从购物车中删除商品项
     */
    @Test
    public void delItemFromCart() {
        jedis.hdel(CART_PRIFIX + "wangbaoqiang", "瓷碗");
        jedis.close();
    }
}

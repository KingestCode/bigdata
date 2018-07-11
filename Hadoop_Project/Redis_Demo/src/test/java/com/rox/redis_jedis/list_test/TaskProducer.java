package com.rox.redis_jedis.list_test;

import java.util.Random;
import java.util.UUID;

import redis.clients.jedis.Jedis;

/**
 *  生产数据, 模拟生产者
 */
public class TaskProducer {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("cs1", 6379);
        Random random = new Random();

        // 生成任务
        while (true) {
            try {
                // 生成任务的速度有一定的随机性，在1-2秒之间
                Thread.sleep(random.nextInt(1000) + 1000);
                // 生成一个任务
                String taskid = UUID.randomUUID().toString();

                // 往任务队列"task-queue"中插入，第一次插入时，"task-queue"还不存在
                // 但是lpush方法会在redis库中创建一条新的list数据
                // 插入到头部
                jedis.lpush("task-queue", taskid);
                System.out.println("向任务队列中插入了一个新的任务: " + taskid);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
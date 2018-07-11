package com.rox.redis_jedis.list_test;

import java.util.Random;

import redis.clients.jedis.Jedis;

/**
 * 描述: 模拟实现消费者任务
 */
public class TaskConsumer {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("cs1", 6379);
        Random random = new Random();

        while (true) {
            try {
                // 从task-queue 尾部 取一个任务，同时放入"temp-queue" 临时队列头部
                String taskid = jedis.rpoplpush("task-queue", "temp-queue");

                // 模拟处理任务
                Thread.sleep(1000);

                // 模拟有成功又有失败的情况
                int nextInt = random.nextInt(10);
                // 模拟失败的情况， 当是8的时候
                if (nextInt >= 8) {
                    // 失败的情况下，需要将任务从"temp-queue" 临时队列尾部  弹回"task-queue"头部
                    // 但是下次还是从 task-queue 的尾部 取任务, 等本次 task-queue的任务执行完了,
                    // 还会执行这个失败的任务
                    jedis.rpoplpush("temp-queue", "task-queue");
                    System.out.println("-------任务处理失败： " + taskid);

                //模拟成功的情况
                } else {
                    // 成功的情况下，将任务从"temp-queue" 尾部 清除
                    jedis.rpop("temp-queue");
                    System.out.println("任务处理成功： " + taskid);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

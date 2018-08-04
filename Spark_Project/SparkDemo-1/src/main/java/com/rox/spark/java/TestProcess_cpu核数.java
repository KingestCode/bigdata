package com.rox.spark.java;

/**
 * 显示当前可用的 cpu 核数
 */
public class TestProcess_cpu核数 {
    public static void main(String[] args) {

        System.out.println(Runtime.getRuntime().availableProcessors());
    }
}

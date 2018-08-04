package com.rox.spark.java.SparkStreaming_java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingCheckpointDemo {
    public static void main(String[] args) throws Exception {

        /**
         * Create a factory object that can create and setup a new JavaStreamingContext
         * 可以使用 Function0
         * A zero-argument function that returns an R.
         */
        Function0<JavaStreamingContext> contextFactory = new Function0<JavaStreamingContext>(){
            @Override
            // 首次创建 context 时, 调用此方法
            public JavaStreamingContext call() throws Exception {
                SparkConf conf = new SparkConf();
                conf.setMaster("local[4]");
                conf.setAppName("SparkStreamingCheckpointDemo");

                // 创建流上下文对象
                JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(2 * 1000));

                // Create an input stream from network source hostname:port.
                JavaDStream<String> lines = jsc.socketTextStream("localhost", 10086);

                // =============== 变换代码 ===============
                // 设置一个窗口时长为1天, 滚动间隔为 2s
                JavaDStream<Long> longJavaDStream = lines.countByWindow(new Duration(24 * 60 * 60 * 1000), new Duration(2 * 1000));

                longJavaDStream.print();

                // 设置检查点 目录
                jsc.checkpoint("file:///Users/shixuanji/Documents/Code/temp/check");

                // 返回流上下文对象
                return jsc;
            }
        };


        /**
         *   def getOrCreate(
         checkpointPath: String,
         creatingFunc: JFunction0[JavaStreamingContext]
         ): JavaStreamingContext
         注意: 第2个参数是一个 Function0对象
         */
        // 失败后, 重新创建时, 会经过检查点
        JavaStreamingContext context = JavaStreamingContext.getOrCreate("file:///Users/shixuanji/Documents/Code/temp/check", contextFactory);


        context.start();
        context.awaitTermination();

    }
}

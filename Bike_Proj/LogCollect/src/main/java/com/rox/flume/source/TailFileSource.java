package com.rox.flume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * 参考源码类
 * @class ExecSource
 * flume source 的生命周期：构造器 -> configure -> start -> processor.process
 * 1.读取配置文件：（配置文件的内容：读取哪个文件、编码集、偏移量写到哪个文件、多长时间检查一下文件是否有新内容）
 */
public class TailFileSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(TailFileSource.class);

    private String filePath;                //监控文件路径
    private String charset;                 //编码集
    private String posiFile;                //存储偏移量文件路径
    private long interval;                  //多长时间检查一下文件是否有新内容, 后面用作: 每拉取一次, 就 sleep 多久
    private ExecutorService executor;       //线程池对象
    private FileRunnable fileRunnable;      //不断读取文件的多线程实现类


    @Override
    public void configure(Context context) {

        filePath = context.getString("filePath");
        charset = context.getString("charset", "UTF-8");
        posiFile = context.getString("posiFile");
        interval = context.getLong("interval", 1000L);
    }

    @Override
    public synchronized void start() {
        //创建一个单线程的线程池
        executor = Executors.newSingleThreadExecutor();
        //定义一个实现 Runnable 接口的类对象
        fileRunnable = new FileRunnable(filePath,charset,posiFile,interval,getChannelProcessor());
        //实现 Runnable 接口的类, 提交到线程池
        executor.submit(fileRunnable);
        //调用父类的 start 方法
        super.start();
    }

    @Override
    public synchronized void stop() {
        fileRunnable.setFlag(false);
        executor.shutdown();

        //如果没有停止成功, 每隔0.5秒询问一次, 直到当次 task 执行完成
        while (!executor.isTerminated()) {
            logger.debug("Waiting for filer executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        super.stop();
    }


    /**
     * 定义一个内部类 FileRunnable 实现 Runnable 接口
     */
    private static class FileRunnable implements Runnable {

        private long interval;
        private String charset;
        private ChannelProcessor channelProcessor;
        private long offset = 0L;
        private RandomAccessFile raf;
        private boolean flag = true;
        private File positionFile;

        /**
         * 构造方法
         * @param filePath  监控的文件的路径
         * @param charset   字符编码
         * @param posiFile  存储偏移量文件的路径
         * @param interval  读取一次 source 的间隔时间
         * @param channelProcessor 通道处理器, 用于批量把事件写入通道
         */
        public FileRunnable(String filePath, String charset, String posiFile, long interval, ChannelProcessor channelProcessor) {

            this.interval = interval;
            this.charset = charset;         // 这里的 charset 是 FileRunnable 在初始化的时候传进来的
            this.channelProcessor = channelProcessor;

            //读取偏移量，如果有，就接着读，没有就从头读
            positionFile = new File(posiFile);
            //不存在存储偏移量的文件, 说明是第一次读, 此时创建一个
            if (!positionFile.exists()) {
                try {
                    positionFile.createNewFile();
                } catch (IOException e) {
                    logger.error("create position file error", e);
                }
            }

            // 存在偏移量文件, 此时读取偏移量
            try {
                //使用 apache.common.io 包中的工具类 FileUtils 读取
                String offsetString = FileUtils.readFileToString(positionFile);
                //如果不为空(记录过偏移量), 就转为 long
                if (offsetString != null && !"".equals(offsetString.trim())) {
                    offset = Long.parseLong(offsetString);
                }

                //创建一个 RandomAccessFile 对象, 用来读取指定偏移量
                raf = new RandomAccessFile(filePath, "r");
                raf.seek(offset);

            } catch (IOException e) {
                logger.error("read position file error", e);
            }
        }

        /**
         * run 方法
         */
        @Override
        public void run() {
            // 控制是否持续读取
            while (flag) {
                try {
                    String line = raf.readLine();
                    if (line != null) {
                        /**
                         * 读取监控文件中的数据
                         * @charsetName: 解码的类型
                         * @charset: 解码后, 转为此种编码
                         */
                        line = new String(line.getBytes("ISO-8859-1"), charset);

                        //  ==> 写入到 channel
                        channelProcessor.processEvent(EventBuilder.withBody(line.getBytes()));

                        //获取最新的偏移量，然后更新偏移量
                        offset = raf.getFilePointer();

                        //将偏移量写入到位置文件中, 覆盖
                        FileUtils.writeStringToFile(positionFile, offset + "", false);
                    } else {
                        Thread.sleep(interval);
                    }

                } catch (IOException e) {
                    logger.error("read file thread error", e);
                } catch (InterruptedException e) {
                    logger.error("sleep interval Interrupted", e);
                }
            }
        }

        public void setFlag(boolean b) {
            this.flag = b;
        }
    }

}

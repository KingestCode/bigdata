package com.rox.NIO;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 结论:  NIO 拷贝文件的时间比 传统 io 要长
 */
public class TestFileChannel {


    private static int bufSize = 512 * 1 ;

    @Test
    public void testAll() throws Exception {
        copyFileNIO();
        copyFileIO();
    }


    /**
     * nio 的文件 copy
     * @throws Exception
     */
    public void copyFileNIO() throws Exception {
        // 输入
        FileInputStream fis = new FileInputStream("/Users/shixuanji/Documents/截图/Snip20180709_231.png");

        // 获得文件通道
        FileChannel fcIn = fis.getChannel();

        // 输出
        FileOutputStream fos = new FileOutputStream("/Users/shixuanji/Documents/截图/nio.png");
        FileChannel fcout = fos.getChannel();

        //////////////
        // 开始计时////
        long start = System.currentTimeMillis();

        // 为 ByteBuffer 分配空间
        ByteBuffer buf = ByteBuffer.allocate(bufSize);

        while (fcIn.read(buf) != -1) {

            buf.flip();  // 缓冲区的排版
            fcout.write(buf);
            buf.clear(); // 缓冲区的清空
        }

        fcIn.close();
        fcout.close();

        System.out.println("nio : " + (System.currentTimeMillis() - start));
    }


    /**
     * 传统 io 的  文件copy
     * @throws Exception
     */
    public void copyFileIO() throws Exception{
        FileInputStream fis = new FileInputStream("/Users/shixuanji/Documents/截图/Snip20180709_231.png");
        FileOutputStream fos = new FileOutputStream("/Users/shixuanji/Documents/截图/io.png");
        long start = System.currentTimeMillis() ;
        byte[] buf = new byte[bufSize] ;
        int len = -1 ;
        while((len = fis.read(buf))!= -1){
            fos.write(buf,0,len);
        }
        fis.close();
        fos.close();
        System.out.println("io : " + (System.currentTimeMillis() - start));
    }


    /**
     * 磁盘文件的内存映射
     * 通过操作内存中的文件映射, 直接修改磁盘中的文件
     * @throws Exception
     */
    @Test
    public void fileMappingMemory() throws Exception {

        File f = new File("/Users/shixuanji/Documents/截图/a.txt");

        // 文件随机访问对象
        // 是可读可写
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        MappedByteBuffer buf = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 1, 9);

        System.out.println((char) buf.get(0));
        System.out.println(buf.capacity());

        buf.put(0, (byte) 'x');
    }


    /**
     * 测试离堆缓冲区, 利用 visualVM :TODO 了解 jvm
     */
    @Test
    public void testOffHeapBuf(){
        System.out.println("xxx");
        ByteBuffer.allocate(500 * 1024 * 1024);             // 分配jvm 虚拟机管理内存
        ByteBuffer.allocateDirect(500 * 1024 * 1024);       // 分配 离堆内存
        System.out.println("xxx");
    }


    /**
     * 反射: 手动释放 离堆内存缓冲区
     */
    @Test
    public void testOffHeapBuf2() throws Exception {

        // 直接分配内存(离堆内存)
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 1024);

        // 得到类描述符
        Class clazz = Class.forName("java.nio.DirectByteBuffer");

        // 通过类描述符查找指定字段 (字段描述符)
        Field f = clazz.getDeclaredField("cleaner");

        // 设置可访问性
        f.setAccessible(true);

        // 取得f 在 buf 上的值
        Object cleaner = f.get(buf);

        System.out.println(cleaner);


        Class clazz2 = Class.forName("sun.misc.Cleaner");
        Method m = clazz2.getDeclaredMethod("clean");
        m.invoke(cleaner);

        System.out.println("kkkkkk");
        System.out.println("kkkkkk");

        // 如果释放了这块离堆内存后, 再访问, 会报一个致命的错误
        /**
         * # A fatal error has been detected by the Java Runtime Environment:
         */

//        System.out.println(buf.get(0));

        System.out.println("kkkkkk");


    }








}
